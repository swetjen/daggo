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
