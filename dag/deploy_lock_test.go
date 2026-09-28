package dag

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDeployLockDetectsAndLatches(t *testing.T) {
	t.Parallel()

	lockPath := filepath.Join(t.TempDir(), "runtime", "WILL_DEPLOY")
	lock := NewDeployLock(lockPath, 5*time.Millisecond, 30*time.Millisecond)

	if lock.IsDraining() {
		t.Fatalf("expected draining=false before lock file exists")
	}

	if err := EnsureDeployLockDir(lockPath); err != nil {
		t.Fatalf("ensure deploy lock dir: %v", err)
	}
	if err := os.WriteFile(lockPath, []byte("deploy"), 0o644); err != nil {
		t.Fatalf("write lock file: %v", err)
	}

	waitFor(t, 200*time.Millisecond, func() bool {
		return lock.IsDraining()
	})

	if err := os.Remove(lockPath); err != nil {
		t.Fatalf("remove lock file: %v", err)
	}
	if !lock.IsDraining() {
		t.Fatalf("expected draining state to remain latched after lock file removal")
	}

	if lock.ShouldForceExit() {
		t.Fatalf("expected force-exit=false immediately after drain starts")
	}
	waitFor(t, 500*time.Millisecond, func() bool {
		return lock.ShouldForceExit()
	})
}

func TestEnsureDeployLockDir(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "a", "b", "WILL_DEPLOY")
	if err := EnsureDeployLockDir(path); err != nil {
		t.Fatalf("ensure deploy lock dir: %v", err)
	}
	info, err := os.Stat(filepath.Dir(path))
	if err != nil {
		t.Fatalf("stat deploy lock dir: %v", err)
	}
	if !info.IsDir() {
		t.Fatalf("expected deploy lock parent to be directory")
	}
}

func waitFor(t *testing.T, maxWait time.Duration, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(maxWait)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", maxWait)
}

func TestDeployLockBeginDrainStartsDrainWithoutLockFile(t *testing.T) {
	t.Parallel()

	lock := NewDeployLock(filepath.Join(t.TempDir(), "WILL_DEPLOY"), time.Second, 40*time.Millisecond)

	select {
	case <-lock.DrainStarted():
		t.Fatalf("drain reported as started before it began")
	default:
	}
	if lock.IsDraining() || lock.ShouldForceExit() {
		t.Fatalf("expected no drain before BeginDrain")
	}

	if !lock.BeginDrain("signal terminated") {
		t.Fatalf("expected BeginDrain to start the drain")
	}
	if lock.BeginDrain("signal terminated") {
		t.Fatalf("expected a second BeginDrain to report the drain was already in progress")
	}

	select {
	case <-lock.DrainStarted():
	default:
		t.Fatalf("expected DrainStarted to be closed once the drain began")
	}
	if !lock.IsDraining() {
		t.Fatalf("expected draining=true after BeginDrain")
	}
	if got := lock.DrainReason(); got != "signal terminated" {
		t.Fatalf("drain reason = %q", got)
	}
	if lock.ShouldForceExit() {
		t.Fatalf("expected the grace period to apply after BeginDrain")
	}
	waitFor(t, 500*time.Millisecond, func() bool {
		return lock.ShouldForceExit()
	})
}

func TestDeployLockForceExitEndsGracePeriodImmediately(t *testing.T) {
	t.Parallel()

	lock := NewDeployLock(filepath.Join(t.TempDir(), "WILL_DEPLOY"), time.Second, time.Hour)
	lock.BeginDrain("signal terminated")
	if lock.ShouldForceExit() {
		t.Fatalf("expected the grace period to apply before ForceExit")
	}

	lock.ForceExit("signal terminated")
	lock.ForceExit("signal terminated")

	select {
	case <-lock.ForceExitRequested():
	default:
		t.Fatalf("expected ForceExitRequested to be closed")
	}
	if !lock.ShouldForceExit() {
		t.Fatalf("expected ForceExit to end the grace period")
	}
}

func TestDeployLockForceExitWithoutDrainStartsOne(t *testing.T) {
	t.Parallel()

	lock := NewDeployLock("", time.Second, time.Hour)
	lock.ForceExit("signal terminated")

	if !lock.IsDraining() || !lock.ShouldForceExit() {
		t.Fatalf("expected ForceExit to start a drain and end its grace period")
	}
}
