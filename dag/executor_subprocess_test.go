package dag

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/swetjen/daggo/db"
)

type subprocessHarness struct {
	t        *testing.T
	ctx      context.Context
	queries  *db.Queries
	executor *Executor
	job      JobDefinition
	dir      string
}

func newSubprocessHarness(t *testing.T, jobKey string, maxConcurrentRuns int) *subprocessHarness {
	t.Helper()

	step := Op[NoInput, exSourceOutput]("op", func(_ context.Context, _ NoInput) (exSourceOutput, error) {
		return exSourceOutput{Value: 1}, nil
	})
	job := NewJob(jobKey).Add(step).MustBuild()

	ctx, queries, executor := newExecutorTestHarness(t, job)
	dir := t.TempDir()
	t.Setenv(testWorkerDirEnv, dir)

	binary, err := os.Executable()
	if err != nil {
		t.Fatalf("resolve test binary: %v", err)
	}
	executor.SetExecutionMode(ExecutionModeSubprocess)
	executor.SetWorkerBinary(binary)
	executor.SetWorkerCommand(testWorkerCommand)
	executor.SetRunMaxConcurrentRuns(maxConcurrentRuns)

	harness := &subprocessHarness{t: t, ctx: ctx, queries: queries, executor: executor, job: job, dir: dir}
	t.Cleanup(func() {
		// Never leave stand-in workers behind, whatever the test outcome.
		executor.InterruptActiveRuns(context.Background(), "test cleanup")
	})
	return harness
}

func (h *subprocessHarness) createRun() db.Run {
	h.t.Helper()
	return createRunWithPendingSteps(h.t, h.ctx, h.queries, h.job.Key, 0, "", "{}")
}

func (h *subprocessHarness) started(runID int64) bool {
	_, err := os.Stat(filepath.Join(h.dir, "started-"+strconv.FormatInt(runID, 10)))
	return err == nil
}

func (h *subprocessHarness) startedCount(runs []db.Run) int {
	count := 0
	for _, run := range runs {
		if h.started(run.ID) {
			count++
		}
	}
	return count
}

func (h *subprocessHarness) release(runID int64) {
	h.t.Helper()
	path := filepath.Join(h.dir, "release-"+strconv.FormatInt(runID, 10))
	if err := os.WriteFile(path, []byte("go"), 0o644); err != nil {
		h.t.Fatalf("release run %d: %v", runID, err)
	}
}

func (h *subprocessHarness) workerPID(runID int64) int {
	h.t.Helper()
	data, err := os.ReadFile(filepath.Join(h.dir, "started-"+strconv.FormatInt(runID, 10)))
	if err != nil {
		h.t.Fatalf("read worker pid for run %d: %v", runID, err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		h.t.Fatalf("parse worker pid for run %d: %v", runID, err)
	}
	return pid
}

func waitForCondition(t *testing.T, timeout time.Duration, description string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	if condition() {
		return
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, description)
}

func runEventTypes(t *testing.T, ctx context.Context, queries *db.Queries, runID int64) map[string]int {
	t.Helper()
	counts := make(map[string]int)
	for _, event := range mustLoadRunEvents(t, ctx, queries, runID) {
		counts[event.EventType]++
	}
	return counts
}

func TestExecutorSubprocess_CapsConcurrentWorkersAndQueuesTheRest(t *testing.T) {
	const maxConcurrentRuns = 2
	const totalRuns = 5

	h := newSubprocessHarness(t, "executor_subprocess_cap", maxConcurrentRuns)

	runs := make([]db.Run, 0, totalRuns)
	for idx := 0; idx < totalRuns; idx++ {
		runs = append(runs, h.createRun())
	}
	for _, run := range runs {
		h.executor.EnqueueRun(run.ID)
	}

	waitForCondition(t, 10*time.Second, "the first two workers to start", func() bool {
		return h.started(runs[0].ID) && h.started(runs[1].ID)
	})

	// Give a third worker every chance to start if the cap were not applied.
	time.Sleep(300 * time.Millisecond)
	if got := h.startedCount(runs); got != maxConcurrentRuns {
		t.Fatalf("expected exactly %d workers started while the cap is full, got %d", maxConcurrentRuns, got)
	}
	if got := h.executor.ActiveRuns(); got != maxConcurrentRuns {
		t.Fatalf("expected %d active runs, got %d", maxConcurrentRuns, got)
	}
	if got := h.executor.QueueDepth(); got != totalRuns-maxConcurrentRuns {
		t.Fatalf("expected %d queued runs, got %d", totalRuns-maxConcurrentRuns, got)
	}
	if h.executor.IsIdle() {
		t.Fatalf("executor must not report idle while runs are active or queued")
	}

	// Free one slot at a time and check that exactly one waiting run starts,
	// in arrival order, without ever exceeding the cap.
	for released := 0; released < totalRuns; released++ {
		h.release(runs[released].ID)

		wantStarted := released + 1 + maxConcurrentRuns
		if wantStarted > totalRuns {
			wantStarted = totalRuns
		}
		waitForCondition(t, 10*time.Second, fmt.Sprintf("%d workers to have started", wantStarted), func() bool {
			return h.startedCount(runs) >= wantStarted
		})
		time.Sleep(100 * time.Millisecond)
		if got := h.startedCount(runs); got != wantStarted {
			t.Fatalf("after releasing %d runs expected %d started workers, got %d", released+1, wantStarted, got)
		}
		for idx := 0; idx < wantStarted; idx++ {
			if !h.started(runs[idx].ID) {
				t.Fatalf("expected run %d (position %d) to start before later runs", runs[idx].ID, idx)
			}
		}
		if got := h.executor.ActiveRuns(); got > maxConcurrentRuns {
			t.Fatalf("active runs %d exceeded the cap %d", got, maxConcurrentRuns)
		}
	}

	waitForCondition(t, 10*time.Second, "the executor to become idle", h.executor.IsIdle)
	if got := h.executor.QueueDepth(); got != 0 {
		t.Fatalf("expected empty queue when idle, got %d", got)
	}

	for idx, run := range runs {
		events := runEventTypes(t, h.ctx, h.queries, run.ID)
		if events["run_worker_started"] != 1 {
			t.Fatalf("run %d: expected one run_worker_started event, got %d", run.ID, events["run_worker_started"])
		}
		if events["run_worker_exited"] != 1 {
			t.Fatalf("run %d: expected one run_worker_exited event, got %d", run.ID, events["run_worker_exited"])
		}
		wantWaiting := 0
		if idx >= maxConcurrentRuns {
			wantWaiting = 1
		}
		if events[RunWorkerWaitingEventType] != wantWaiting {
			t.Fatalf("run %d (position %d): expected %d %s events, got %d", run.ID, idx, wantWaiting, RunWorkerWaitingEventType, events[RunWorkerWaitingEventType])
		}
	}
}

func TestExecutorSubprocess_CapOfOneRunsSerially(t *testing.T) {
	h := newSubprocessHarness(t, "executor_subprocess_serial", 1)

	first := h.createRun()
	second := h.createRun()
	h.executor.EnqueueRun(first.ID)
	h.executor.EnqueueRun(second.ID)

	waitForCondition(t, 10*time.Second, "the first worker to start", func() bool { return h.started(first.ID) })
	time.Sleep(300 * time.Millisecond)
	if h.started(second.ID) {
		t.Fatalf("second run started while the only slot was in use")
	}

	h.release(first.ID)
	waitForCondition(t, 10*time.Second, "the second worker to start", func() bool { return h.started(second.ID) })
	h.release(second.ID)
	waitForCondition(t, 10*time.Second, "the executor to become idle", h.executor.IsIdle)
}

func TestExecutorSubprocess_TerminatingQueuedRunNeverStartsAWorker(t *testing.T) {
	h := newSubprocessHarness(t, "executor_subprocess_cancel_queued", 1)

	first := h.createRun()
	second := h.createRun()
	third := h.createRun()
	h.executor.EnqueueRun(first.ID)
	h.executor.EnqueueRun(second.ID)
	h.executor.EnqueueRun(third.ID)

	waitForCondition(t, 10*time.Second, "the first worker to start", func() bool { return h.started(first.ID) })

	if err := h.executor.TerminateRun(second.ID); err != nil {
		t.Fatalf("terminate queued run: %v", err)
	}
	if got := h.executor.QueueDepth(); got != 1 {
		t.Fatalf("expected the terminated run to leave the queue, depth=%d", got)
	}

	h.release(first.ID)
	waitForCondition(t, 10*time.Second, "the third worker to start", func() bool { return h.started(third.ID) })
	h.release(third.ID)
	waitForCondition(t, 10*time.Second, "the executor to become idle", h.executor.IsIdle)

	if h.started(second.ID) {
		t.Fatalf("a worker was started for a run terminated while queued")
	}
	row, err := h.queries.RunGetByID(h.ctx, second.ID)
	if err != nil {
		t.Fatalf("load terminated run: %v", err)
	}
	if row.Status != "canceled" {
		t.Fatalf("expected terminated run to be canceled, got %q", row.Status)
	}
}
