package runs

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/swetjen/daggo/dag"
	"github.com/swetjen/daggo/deps"
	"github.com/swetjen/virtuous/rpc"
)

func TestRunCreateBlockedWhenDeployDraining(t *testing.T) {
	t.Parallel()

	lockPath := filepath.Join(t.TempDir(), "runtime", "WILL_DEPLOY")
	if err := dag.EnsureDeployLockDir(lockPath); err != nil {
		t.Fatalf("ensure deploy lock dir: %v", err)
	}
	if err := os.WriteFile(lockPath, []byte("deploy"), 0o644); err != nil {
		t.Fatalf("write lock file: %v", err)
	}
	lock := dag.NewDeployLock(lockPath, 5*time.Millisecond, 30*time.Second)

	h := New(&deps.Deps{DeployLock: lock})
	resp, status := h.RunCreate(context.Background(), RunCreateRequest{
		JobKey: "any_job",
	})
	if status != rpc.StatusInvalid {
		t.Fatalf("expected status invalid while deploy draining, got %d", status)
	}
	if resp.Error != deployDrainErrorMessage {
		t.Fatalf("expected deploy drain error message, got %q", resp.Error)
	}
}

func TestRunRerunStepCreateBlockedWhenDeployDraining(t *testing.T) {
	t.Parallel()

	lockPath := filepath.Join(t.TempDir(), "runtime", "WILL_DEPLOY")
	if err := dag.EnsureDeployLockDir(lockPath); err != nil {
		t.Fatalf("ensure deploy lock dir: %v", err)
	}
	if err := os.WriteFile(lockPath, []byte("deploy"), 0o644); err != nil {
		t.Fatalf("write lock file: %v", err)
	}
	lock := dag.NewDeployLock(lockPath, 5*time.Millisecond, 30*time.Second)

	h := New(&deps.Deps{DeployLock: lock})
	resp, status := h.RunRerunStepCreate(context.Background(), RunRerunStepCreateRequest{
		SourceRunID: 123,
		StepKey:     "step_key",
	})
	if status != rpc.StatusInvalid {
		t.Fatalf("expected status invalid while deploy draining, got %d", status)
	}
	if resp.Error != deployDrainErrorMessage {
		t.Fatalf("expected deploy drain error message, got %q", resp.Error)
	}
}
