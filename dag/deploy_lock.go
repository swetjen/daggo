package dag

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type DeployLock struct {
	path         string
	pollInterval time.Duration
	gracePeriod  time.Duration

	nowFn func() time.Time

	mu            sync.Mutex
	lastCheckedAt time.Time
	draining      bool
	drainStarted  time.Time
	drainReason   string
	forceExit     bool
	drainCh       chan struct{}
	forceCh       chan struct{}
}

func NewDeployLock(path string, pollInterval, gracePeriod time.Duration) *DeployLock {
	if pollInterval <= 0 {
		pollInterval = time.Second
	}
	normalizedPath := strings.TrimSpace(path)
	return &DeployLock{
		path:         normalizedPath,
		pollInterval: pollInterval,
		gracePeriod:  gracePeriod,
		nowFn:        time.Now,
		drainCh:      make(chan struct{}),
		forceCh:      make(chan struct{}),
	}
}

// BeginDrain starts a drain without the lock file, for example when the
// process receives a termination signal. It has the same effect as the lock
// file appearing: new runs are blocked, the scheduler stops creating runs, and
// the grace period starts counting. It reports whether this call started the
// drain; it returns false when a drain was already in progress.
func (d *DeployLock) BeginDrain(reason string) bool {
	if d == nil {
		return false
	}
	now := d.nowFn().UTC()
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.draining {
		return false
	}
	d.startDrainLocked(now, strings.TrimSpace(reason))
	slog.Warn("daggo: drain started", "reason", d.drainReason, "drain_started_at", now.Format(time.RFC3339Nano))
	return true
}

// ForceExit ends the grace period immediately. It starts a drain first when
// none is in progress.
func (d *DeployLock) ForceExit(reason string) {
	if d == nil {
		return
	}
	now := d.nowFn().UTC()
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.draining {
		d.startDrainLocked(now, strings.TrimSpace(reason))
	}
	if d.forceExit {
		return
	}
	d.forceExit = true
	if d.forceCh != nil {
		close(d.forceCh)
	}
	slog.Warn("daggo: drain grace period ended early", "reason", strings.TrimSpace(reason))
}

// DrainStarted returns a channel that is closed when a drain begins.
func (d *DeployLock) DrainStarted() <-chan struct{} {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.drainCh
}

// ForceExitRequested returns a channel that is closed when ForceExit is
// called.
func (d *DeployLock) ForceExitRequested() <-chan struct{} {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.forceCh
}

// DrainReason reports why the current drain started, or "" when not draining.
func (d *DeployLock) DrainReason() string {
	if d == nil {
		return ""
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.drainReason
}

// startDrainLocked marks the drain as started. The caller holds d.mu.
func (d *DeployLock) startDrainLocked(now time.Time, reason string) {
	if reason == "" {
		reason = "drain requested"
	}
	d.draining = true
	d.drainStarted = now
	d.drainReason = reason
	if d.drainCh != nil {
		close(d.drainCh)
	}
}

func (d *DeployLock) Path() string {
	if d == nil {
		return ""
	}
	return d.path
}

func (d *DeployLock) PollInterval() time.Duration {
	if d == nil {
		return time.Second
	}
	return d.pollInterval
}

func (d *DeployLock) GracePeriod() time.Duration {
	if d == nil {
		return 0
	}
	return d.gracePeriod
}

func (d *DeployLock) IsDraining() bool {
	if d == nil {
		return false
	}
	d.refreshIfNeeded(d.nowFn().UTC())
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.draining
}

func (d *DeployLock) DrainStartedAt() (time.Time, bool) {
	if d == nil {
		return time.Time{}, false
	}
	d.refreshIfNeeded(d.nowFn().UTC())
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.drainStarted.IsZero() {
		return time.Time{}, false
	}
	return d.drainStarted, true
}

func (d *DeployLock) ShouldForceExit() bool {
	if d == nil {
		return false
	}
	d.refreshIfNeeded(d.nowFn().UTC())
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.draining || d.drainStarted.IsZero() {
		return false
	}
	if d.forceExit {
		return true
	}
	if d.gracePeriod <= 0 {
		return false
	}
	return d.nowFn().UTC().Sub(d.drainStarted) >= d.gracePeriod
}

func (d *DeployLock) refreshIfNeeded(now time.Time) {
	d.mu.Lock()
	if d.path == "" {
		d.mu.Unlock()
		return
	}
	if d.draining {
		d.mu.Unlock()
		return
	}
	if !d.lastCheckedAt.IsZero() && now.Sub(d.lastCheckedAt) < d.pollInterval {
		d.mu.Unlock()
		return
	}
	d.lastCheckedAt = now
	path := d.path
	d.mu.Unlock()

	if !fileExists(path) {
		return
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	if d.draining {
		return
	}
	d.startDrainLocked(now, "deploy lock file "+path)
	slog.Warn("daggo: deploy drain lock detected", "path", path, "drain_started_at", now.Format(time.RFC3339Nano))
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func EnsureDeployLockDir(path string) error {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return nil
	}
	dir := filepath.Dir(trimmed)
	if dir == "." || dir == "" {
		return nil
	}
	return os.MkdirAll(dir, 0o755)
}
