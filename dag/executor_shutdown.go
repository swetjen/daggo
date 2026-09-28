package dag

import (
	"context"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/swetjen/daggo/db"
)

const workerExitWaitTimeout = 5 * time.Second

// InterruptActiveRuns ends every run this executor still owns and stops it
// from starting more. It is the last step of a shutdown, after the drain grace
// period: worker processes are terminated, in-process runs have their context
// cancelled, and each run that had not finished, including runs still waiting
// for a slot, is recorded as failed with the given reason. It returns the IDs
// of the runs it acted on.
func (e *Executor) InterruptActiveRuns(ctx context.Context, reason string) []int64 {
	if e == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = RunInterruptedReason
	}

	affected := make(map[int64]struct{})

	e.processesMu.Lock()
	e.stopped = true
	e.stoppedReason = reason
	for _, runID := range e.subprocessPending {
		affected[runID] = struct{}{}
	}
	e.subprocessPending = nil
	for runID := range e.inProcessActive {
		affected[runID] = struct{}{}
	}
	e.processesMu.Unlock()

drainQueue:
	for {
		select {
		case runID, ok := <-e.queue:
			if !ok {
				break drainQueue
			}
			affected[runID] = struct{}{}
		default:
			break drainQueue
		}
	}

	for _, runID := range e.terminateWorkers(ctx, reason) {
		affected[runID] = struct{}{}
	}

	runIDs := make([]int64, 0, len(affected))
	for runID := range affected {
		runIDs = append(runIDs, runID)
	}
	sort.Slice(runIDs, func(i, j int) bool { return runIDs[i] < runIDs[j] })

	for _, runID := range runIDs {
		if err := e.markRunInterrupted(ctx, runID, reason); err != nil {
			slog.Error("daggo: failed to mark run interrupted", "run_id", runID, "err", err)
		}
	}
	// Cancel in-process runs only after their outcome is recorded, so a step
	// that reacts to the cancellation cannot race the interrupted status.
	if e.runCancel != nil {
		e.runCancel()
	}
	return runIDs
}

// terminateWorkers kills every worker process and waits, bounded, for their
// exits to be recorded. It returns the run IDs whose workers were killed.
func (e *Executor) terminateWorkers(ctx context.Context, reason string) []int64 {
	killed := make(map[int64]struct{})
	deadline := time.Now().Add(workerExitWaitTimeout)
	for {
		e.processesMu.Lock()
		targets := make(map[int64]runProcess)
		for runID, process := range e.processes {
			if _, done := killed[runID]; done {
				continue
			}
			targets[runID] = process
			if _, terminated := e.terminated[runID]; !terminated {
				e.interrupted[runID] = reason
			}
		}
		remaining := len(e.processes)
		slots := e.subprocessSlots
		e.processesMu.Unlock()

		for runID, process := range targets {
			killed[runID] = struct{}{}
			if err := killWorkerProcess(process.pid); err != nil {
				slog.Error("daggo: failed to terminate run worker", "run_id", runID, "pid", process.pid, "err", err)
				continue
			}
			slog.Warn("daggo: terminated run worker for shutdown", "run_id", runID, "pid", process.pid)
			_ = e.addEvent(ctx, runID, "", "run_worker_terminated", "warn", "worker process terminated", map[string]any{
				"pid":    process.pid,
				"reason": reason,
			})
		}

		if remaining == 0 && slots == 0 {
			break
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			slog.Error("daggo: timed out waiting for run workers to exit", "remaining", remaining)
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	runIDs := make([]int64, 0, len(killed))
	for runID := range killed {
		runIDs = append(runIDs, runID)
	}
	return runIDs
}

// markRunInterrupted records a run that a shutdown ended before it finished.
// The run is stored as failed with the reason as its error message. Steps that
// were executing are stored as failed and steps that never started as skipped.
// A run that already reached a terminal status is left unchanged.
func (e *Executor) markRunInterrupted(ctx context.Context, runID int64, reason string) error {
	if e == nil || runID <= 0 {
		return nil
	}
	message := strings.TrimSpace(reason)
	if message == "" {
		message = RunInterruptedReason
	}

	run, err := e.queries.RunGetByID(ctx, runID)
	if err != nil {
		return err
	}
	previousStatus := normalizeExecutionStatus(run.Status)
	if isTerminalExecutionStatus(previousStatus) {
		return nil
	}

	now := time.Now().UTC()
	completedAt := now.Format(time.RFC3339Nano)
	if _, err := e.queries.RunUpdateForComplete(ctx, db.RunUpdateForCompleteParams{
		Status:       "failed",
		CompletedAt:  completedAt,
		ErrorMessage: message,
		ID:           runID,
	}); err != nil {
		return err
	}

	steps, err := e.queries.RunStepGetManyByRunID(ctx, runID)
	if err == nil {
		for _, step := range steps {
			status := normalizeExecutionStatus(step.Status)
			if status == "success" || status == "failed" || status == "skipped" || status == "canceled" {
				continue
			}
			outputJSON := strings.TrimSpace(step.OutputJson)
			if outputJSON == "" {
				outputJSON = "{}"
			}
			if status != "running" {
				_, _ = e.queries.RunStepUpdateForComplete(ctx, db.RunStepUpdateForCompleteParams{
					Status:       "skipped",
					CompletedAt:  completedAt,
					DurationMs:   0,
					OutputJson:   outputJSON,
					ErrorMessage: message,
					LogExcerpt:   message,
					RunID:        runID,
					StepKey:      step.StepKey,
				})
				_ = e.addEvent(ctx, runID, step.StepKey, "step_skipped", "warn", "step skipped: "+message, map[string]any{
					"reason": message,
				})
				continue
			}
			durationMs := stepDurationAt(step, now)
			_, _ = e.queries.RunStepUpdateForComplete(ctx, db.RunStepUpdateForCompleteParams{
				Status:       "failed",
				CompletedAt:  completedAt,
				DurationMs:   durationMs,
				OutputJson:   outputJSON,
				ErrorMessage: message,
				LogExcerpt:   message,
				RunID:        runID,
				StepKey:      step.StepKey,
			})
			_ = e.addEvent(ctx, runID, step.StepKey, StepInterruptedEventType, "warn", "step "+message, map[string]any{
				"reason":      message,
				"duration_ms": durationMs,
			})
		}
	}

	_ = e.addEvent(ctx, runID, "", RunInterruptedEventType, "warn", "run "+message, map[string]any{
		"reason":          message,
		"previous_status": previousStatus,
	})
	_ = SyncLinkedQueueItemStatus(ctx, e.queries, runID)
	return nil
}
