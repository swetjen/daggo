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
	if d.gracePeriod <= 0 {
		return false
	}
	d.refreshIfNeeded(d.nowFn().UTC())
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.draining || d.drainStarted.IsZero() {
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
	d.draining = true
	d.drainStarted = now
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
