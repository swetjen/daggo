package dag

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/swetjen/daggo/db"
)

func assertRunInterrupted(t *testing.T, ctx context.Context, queries *db.Queries, runID int64, reason string) {
	t.Helper()

	row, err := queries.RunGetByID(ctx, runID)
	if err != nil {
		t.Fatalf("load run %d: %v", runID, err)
	}
	if row.Status != "failed" {
		t.Fatalf("run %d: expected interrupted run to be stored as failed, got %q", runID, row.Status)
	}
	if row.ErrorMessage != reason {
		t.Fatalf("run %d: expected error message %q, got %q", runID, reason, row.ErrorMessage)
	}
	if row.CompletedAt == "" {
		t.Fatalf("run %d: expected completed_at to be set", runID)
	}
	events := runEventTypes(t, ctx, queries, runID)
	if events[RunInterruptedEventType] != 1 {
		t.Fatalf("run %d: expected one %s event, got %d", runID, RunInterruptedEventType, events[RunInterruptedEventType])
	}
}

func TestExecutorInterruptActiveRuns_SubprocessKillsWorkersAndMarksRuns(t *testing.T) {
	h := newSubprocessHarness(t, "executor_interrupt_subprocess", 1)

	running := h.createRun()
	queued := h.createRun()
	h.executor.EnqueueRun(running.ID)
	h.executor.EnqueueRun(queued.ID)
	waitForCondition(t, 10*time.Second, "the worker to start", func() bool { return h.started(running.ID) })

	// The stand-in worker does not write run state, so record what a real
	// worker would have written by this point.
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := h.queries.RunUpdateForStart(h.ctx, db.RunUpdateForStartParams{Status: "running", StartedAt: now, ID: running.ID}); err != nil {
		t.Fatalf("mark run running: %v", err)
	}
	if _, err := h.queries.RunStepUpdateForStart(h.ctx, db.RunStepUpdateForStartParams{Status: "running", StartedAt: now, RunID: running.ID, StepKey: "op"}); err != nil {
		t.Fatalf("mark step running: %v", err)
	}
	pid := h.workerPID(running.ID)
	if !processAlive(pid) {
		t.Fatalf("expected worker pid %d to be alive before shutdown", pid)
	}

	affected := h.executor.InterruptActiveRuns(h.ctx, RunInterruptedReason)
	if len(affected) != 2 {
		t.Fatalf("expected 2 runs to be interrupted, got %v", affected)
	}

	waitForCondition(t, 5*time.Second, "the worker process to be gone", func() bool { return !processAlive(pid) })
	if !h.executor.IsIdle() {
		t.Fatalf("expected executor to be idle after interrupting runs; active=%d queued=%d", h.executor.ActiveRuns(), h.executor.QueueDepth())
	}

	assertRunInterrupted(t, h.ctx, h.queries, running.ID, RunInterruptedReason)
	assertRunInterrupted(t, h.ctx, h.queries, queued.ID, RunInterruptedReason)
	if h.started(queued.ID) {
		t.Fatalf("a worker was started for the queued run during shutdown")
	}

	runningSteps, err := h.queries.RunStepGetManyByRunID(h.ctx, running.ID)
	if err != nil {
		t.Fatalf("load steps: %v", err)
	}
	if step := mustFindRunStep(t, runningSteps, "op"); step.Status != "failed" || step.ErrorMessage != RunInterruptedReason {
		t.Fatalf("expected executing step to be failed with the interrupt reason, got status=%q error=%q", step.Status, step.ErrorMessage)
	}
	queuedSteps, err := h.queries.RunStepGetManyByRunID(h.ctx, queued.ID)
	if err != nil {
		t.Fatalf("load steps: %v", err)
	}
	if step := mustFindRunStep(t, queuedSteps, "op"); step.Status != "skipped" || step.ErrorMessage != RunInterruptedReason {
		t.Fatalf("expected unstarted step to be skipped with the interrupt reason, got status=%q error=%q", step.Status, step.ErrorMessage)
	}

	events := runEventTypes(t, h.ctx, h.queries, running.ID)
	if events["run_worker_terminated"] != 1 {
		t.Fatalf("expected one run_worker_terminated event, got %d", events["run_worker_terminated"])
	}
	if events[StepInterruptedEventType] != 1 {
		t.Fatalf("expected one %s event, got %d", StepInterruptedEventType, events[StepInterruptedEventType])
	}
	if events["run_canceled"] != 0 || events["run_failed"] != 0 {
		t.Fatalf("interrupted run must not also be reported as canceled or failed by the worker-exit path: %v", events)
	}

	// Nothing may start once the executor has been stopped.
	late := h.createRun()
	h.executor.EnqueueRun(late.ID)
	time.Sleep(200 * time.Millisecond)
	if h.started(late.ID) {
		t.Fatalf("a worker was started after shutdown")
	}
	assertRunInterrupted(t, h.ctx, h.queries, late.ID, RunInterruptedReason)
}

func TestExecutorInterruptActiveRuns_InProcessMarksRunsAndCancelsContext(t *testing.T) {
	started := make(chan struct{}, 1)
	stepReturned := make(chan error, 1)

	step := Op[NoInput, exSourceOutput]("op", func(ctx context.Context, _ NoInput) (exSourceOutput, error) {
		started <- struct{}{}
		select {
		case <-ctx.Done():
			stepReturned <- ctx.Err()
			return exSourceOutput{}, ctx.Err()
		case <-time.After(20 * time.Second):
			stepReturned <- fmt.Errorf("step context was never cancelled")
			return exSourceOutput{Value: 1}, nil
		}
	})
	job := NewJob("executor_interrupt_in_process").Add(step).MustBuild()

	ctx, queries, executor := newExecutorTestHarness(t, job)
	executor.SetRunMaxConcurrentRuns(1)
	t.Cleanup(func() { close(executor.queue) })

	running := createRunWithPendingSteps(t, ctx, queries, job.Key, 0, "", "{}")
	queued := createRunWithPendingSteps(t, ctx, queries, job.Key, 0, "", "{}")
	executor.EnqueueRun(running.ID)
	executor.EnqueueRun(queued.ID)

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatalf("step did not start")
	}

	affected := executor.InterruptActiveRuns(ctx, RunInterruptedReason)
	if len(affected) != 2 {
		t.Fatalf("expected 2 runs to be interrupted, got %v", affected)
	}

	select {
	case err := <-stepReturned:
		if err != context.Canceled {
			t.Fatalf("expected the step context to be cancelled, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("step did not observe cancellation")
	}
	waitForCondition(t, 5*time.Second, "the executor to become idle", executor.IsIdle)

	// The step returning after the interrupt must not overwrite the outcome.
	assertRunInterrupted(t, ctx, queries, running.ID, RunInterruptedReason)
	assertRunInterrupted(t, ctx, queries, queued.ID, RunInterruptedReason)

	events := runEventTypes(t, ctx, queries, queued.ID)
	if events["run_started"] != 0 {
		t.Fatalf("queued run must not start during shutdown")
	}
}

func TestExecutorMarkRunInterrupted_LeavesTerminalRunsUnchanged(t *testing.T) {
	step := Op[NoInput, exSourceOutput]("op", func(_ context.Context, _ NoInput) (exSourceOutput, error) {
		return exSourceOutput{Value: 1}, nil
	})
	job := NewJob("executor_interrupt_terminal").Add(step).MustBuild()

	ctx, queries, executor := newExecutorTestHarness(t, job)
	run := createRunWithPendingSteps(t, ctx, queries, job.Key, 0, "", "{}")
	if err := executor.executeRun(ctx, run.ID); err != nil {
		t.Fatalf("execute run: %v", err)
	}

	if err := executor.markRunInterrupted(ctx, run.ID, RunInterruptedReason); err != nil {
		t.Fatalf("mark interrupted: %v", err)
	}

	row, err := queries.RunGetByID(ctx, run.ID)
	if err != nil {
		t.Fatalf("load run: %v", err)
	}
	if row.Status != "success" || row.ErrorMessage != "" {
		t.Fatalf("expected finished run to stay successful, got status=%q error=%q", row.Status, row.ErrorMessage)
	}
	if events := runEventTypes(t, ctx, queries, run.ID); events[RunInterruptedEventType] != 0 {
		t.Fatalf("finished run must not receive a %s event", RunInterruptedEventType)
	}
}
