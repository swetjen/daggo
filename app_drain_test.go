package daggo

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/swetjen/daggo/dag"
)

type drainFixtureOutput struct {
	Value string `json:"value"`
}

// TestDeployLockFileDrainInterruptsRunsThatOutliveTheGracePeriod proves the
// lock-file drain and the signal drain are one path: a drain started by the
// lock file also interrupts runs that are still in flight when the grace
// period ends, instead of leaving them in "running".
func TestDeployLockFileDrainInterruptsRunsThatOutliveTheGracePeriod(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	started := make(chan struct{}, 1)
	step := dag.Op[dag.NoInput, drainFixtureOutput](
		"blocking_step",
		func(ctx context.Context, _ dag.NoInput) (drainFixtureOutput, error) {
			started <- struct{}{}
			select {
			case <-ctx.Done():
				return drainFixtureOutput{}, ctx.Err()
			case <-time.After(30 * time.Second):
				return drainFixtureOutput{Value: "finished"}, nil
			}
		},
	)
	job := dag.NewJob("drain_fixture").Add(step).MustBuild()

	cfg := DefaultConfig()
	cfg.Database.SQLite.Path = filepath.Join(dir, "daggo.sqlite")
	cfg.Execution.Mode = dag.ExecutionModeInProcess
	cfg.Deploy.LockPath = filepath.Join(dir, "runtime", "WILL_DEPLOY")
	cfg.Deploy.PollSeconds = 1
	cfg.Deploy.DrainGraceSeconds = 1

	app, err := Open(ctx, cfg, job)
	if err != nil {
		t.Fatalf("open app: %v", err)
	}
	t.Cleanup(func() { _ = app.Close() })

	jobRow, err := app.Deps().DB.JobGetByKey(ctx, job.Key)
	if err != nil {
		t.Fatalf("load job: %v", err)
	}
	run, _, err := dag.InsertQueuedRun(ctx, app.Deps().DB, dag.RunInsertInput{JobID: jobRow.ID, TriggeredBy: "drain-test"})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	app.Deps().Executor.EnqueueRun(run.ID)
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatalf("run did not start")
	}

	if err := os.WriteFile(cfg.Deploy.LockPath, []byte("deploy"), 0o644); err != nil {
		t.Fatalf("write deploy lock: %v", err)
	}

	select {
	case <-app.drainDone:
	case <-time.After(10 * time.Second):
		t.Fatalf("drain did not finish")
	}

	// The pool stays open until Close, so the outcome can be read here.
	row, err := app.Deps().DB.RunGetByID(ctx, run.ID)
	if err != nil {
		t.Fatalf("load run: %v", err)
	}
	if row.Status != "failed" || row.ErrorMessage != dag.RunInterruptedReason {
		t.Fatalf("run status=%q error=%q, want failed / %q", row.Status, row.ErrorMessage, dag.RunInterruptedReason)
	}
	idleDeadline := time.Now().Add(5 * time.Second)
	for !app.Deps().Executor.IsIdle() && time.Now().Before(idleDeadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !app.Deps().Executor.IsIdle() {
		t.Fatalf("expected the executor to be idle after the drain")
	}
	select {
	case <-app.runtime.Done():
	default:
		t.Fatalf("expected the runtime context to be cancelled after the drain")
	}
}

func TestDrainShutsDownImmediatelyWhenIdle(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	cfg := DefaultConfig()
	cfg.Database.SQLite.Path = filepath.Join(dir, "daggo.sqlite")
	cfg.Execution.Mode = dag.ExecutionModeInProcess
	cfg.Deploy.LockPath = filepath.Join(dir, "WILL_DEPLOY")
	cfg.Deploy.DrainGraceSeconds = 600

	app, err := Open(ctx, cfg)
	if err != nil {
		t.Fatalf("open app: %v", err)
	}
	t.Cleanup(func() { _ = app.Close() })

	app.Deps().DeployLock.BeginDrain("signal terminated")

	select {
	case <-app.drainDone:
	case <-time.After(5 * time.Second):
		t.Fatalf("an idle server did not shut down promptly after the drain began")
	}
}
