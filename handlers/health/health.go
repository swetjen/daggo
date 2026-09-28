// Package health serves the unauthenticated liveness endpoint.
package health

import (
	"context"
	"net/http"
	"time"

	"github.com/swetjen/daggo/dag"
	"github.com/swetjen/daggo/deps"
	"github.com/swetjen/virtuous/httpapi"
)

const (
	// Path is where the health endpoint is served.
	Path = "/healthz"

	StatusOK        = "ok"
	StatusDraining  = "draining"
	StatusUnhealthy = "unhealthy"

	SchedulerStateDisabled = "disabled"

	databasePingTimeout = 2 * time.Second
)

type Database struct {
	Reachable bool `json:"reachable"`
}

type Scheduler struct {
	Enabled bool `json:"enabled"`
	Alive   bool `json:"alive"`
	// State is "disabled", "not_started", "running", "stalled", or "stopped".
	State string `json:"state"`
	// LastTickAt is the RFC 3339 time of the most recent scheduler tick, or
	// empty before the first tick.
	LastTickAt string `json:"last_tick_at"`
}

// Response is the body of the health endpoint. It deliberately carries no
// error text, configuration, or version: the endpoint is unauthenticated.
type Response struct {
	// Status is "ok", "draining", or "unhealthy".
	Status    string    `json:"status"`
	Database  Database  `json:"database"`
	Scheduler Scheduler `json:"scheduler"`
	// Draining is true once a shutdown or deploy drain has begun. A draining
	// server is still healthy: it answers 200 while in-flight runs finish.
	Draining bool `json:"draining"`
}

type Handlers struct {
	app *deps.Deps
}

func New(app *deps.Deps) *Handlers {
	return &Handlers{app: app}
}

// Healthz answers 200 when the database is reachable and the scheduler, if
// enabled, is alive. It answers 503 otherwise.
func (h *Handlers) Healthz(w http.ResponseWriter, r *http.Request) {
	resp := h.Check(r.Context())
	status := http.StatusOK
	if resp.Status == StatusUnhealthy {
		status = http.StatusServiceUnavailable
	}
	w.Header().Set("Cache-Control", "no-store")
	httpapi.Encode(w, r, status, resp)
}

// Check evaluates the health of the runtime.
func (h *Handlers) Check(ctx context.Context) Response {
	resp := Response{
		Status:    StatusUnhealthy,
		Scheduler: Scheduler{State: SchedulerStateDisabled},
	}
	if h == nil || h.app == nil {
		return resp
	}
	if ctx == nil {
		ctx = context.Background()
	}

	resp.Database.Reachable = h.databaseReachable(ctx)
	resp.Scheduler = h.scheduler()
	resp.Draining = h.app.DeployLock != nil && h.app.DeployLock.IsDraining()

	healthy := resp.Database.Reachable && (!resp.Scheduler.Enabled || resp.Scheduler.Alive)
	switch {
	case !healthy:
		resp.Status = StatusUnhealthy
	case resp.Draining:
		resp.Status = StatusDraining
	default:
		resp.Status = StatusOK
	}
	return resp
}

func (h *Handlers) databaseReachable(ctx context.Context) bool {
	if h.app.Pool == nil {
		return false
	}
	pingCtx, cancel := context.WithTimeout(ctx, databasePingTimeout)
	defer cancel()
	return h.app.Pool.PingContext(pingCtx) == nil
}

func (h *Handlers) scheduler() Scheduler {
	if !h.app.Config.Scheduler.Enabled {
		return Scheduler{State: SchedulerStateDisabled}
	}
	out := Scheduler{Enabled: true, State: dag.SchedulerStateNotStarted}
	if h.app.Scheduler == nil {
		return out
	}
	health := h.app.Scheduler.Health()
	out.Alive = health.Alive
	out.State = health.State
	if !health.LastTickAt.IsZero() {
		out.LastTickAt = health.LastTickAt.UTC().Format(time.RFC3339Nano)
	}
	return out
}
