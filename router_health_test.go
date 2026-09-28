package daggo_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/swetjen/daggo"
	"github.com/swetjen/daggo/dag"
	"github.com/swetjen/daggo/db"
	"github.com/swetjen/daggo/handlers/health"
)

func healthTestConfig(t *testing.T) daggo.Config {
	t.Helper()

	cfg := daggo.DefaultConfig()
	cfg.Database.SQLite.Path = filepath.Join(t.TempDir(), "daggo.sqlite")
	cfg.Execution.Mode = dag.ExecutionModeInProcess
	cfg.Deploy.LockPath = filepath.Join(t.TempDir(), "WILL_DEPLOY")
	return cfg
}

func getHealth(t *testing.T, handler http.Handler, header http.Header) (*httptest.ResponseRecorder, health.Response) {
	t.Helper()

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	for key, values := range header {
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	var body health.Response
	if rec.Code == http.StatusOK || rec.Code == http.StatusServiceUnavailable {
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode health body %q: %v", rec.Body.String(), err)
		}
	}
	return rec, body
}

func waitForSchedulerTick(t *testing.T, handler http.Handler) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		_, body := getHealth(t, handler, nil)
		if body.Scheduler.LastTickAt != "" {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("scheduler never ticked")
}

func TestHealthzIsServedWithoutBearerTokenAndWithoutUI(t *testing.T) {
	cfg := healthTestConfig(t)
	cfg.Admin.SecretKey = "health-test-secret"
	cfg.DisableUI = true

	app, err := daggo.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("open app: %v", err)
	}
	t.Cleanup(func() { _ = app.Close() })
	handler := app.Handler()
	waitForSchedulerTick(t, handler)

	rec, body := getHealth(t, handler, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /healthz without a token = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if contentType := rec.Header().Get("Content-Type"); !strings.HasPrefix(contentType, "application/json") {
		t.Fatalf("expected a JSON body, got content type %q", contentType)
	}
	if body.Status != health.StatusOK {
		t.Fatalf("status = %q, want %q", body.Status, health.StatusOK)
	}
	if !body.Database.Reachable {
		t.Fatalf("expected database to be reachable")
	}
	if !body.Scheduler.Enabled || !body.Scheduler.Alive || body.Scheduler.State != dag.SchedulerStateRunning {
		t.Fatalf("expected a running scheduler, got %+v", body.Scheduler)
	}
	if body.Draining {
		t.Fatalf("expected draining to be false")
	}
	if strings.Contains(rec.Body.String(), cfg.Admin.SecretKey) {
		t.Fatalf("health body must not contain the admin secret")
	}

	// The secret still guards the RPC surface, and the UI is still off.
	rpcReq := httptest.NewRequest(http.MethodPost, "/rpc/system/info-get", strings.NewReader("{}"))
	rpcRec := httptest.NewRecorder()
	handler.ServeHTTP(rpcRec, rpcReq)
	if rpcRec.Code != http.StatusUnauthorized {
		t.Fatalf("POST /rpc/system/info-get without a token = %d, want 401", rpcRec.Code)
	}
	uiReq := httptest.NewRequest(http.MethodGet, "/", nil)
	uiRec := httptest.NewRecorder()
	handler.ServeHTTP(uiRec, uiReq)
	if uiRec.Code != http.StatusNotFound {
		t.Fatalf("GET / with the UI disabled = %d, want 404", uiRec.Code)
	}
}

func TestHealthzIsServedWithUIEnabledAndNoSecret(t *testing.T) {
	cfg := healthTestConfig(t)

	app, err := daggo.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("open app: %v", err)
	}
	t.Cleanup(func() { _ = app.Close() })

	rec, body := getHealth(t, app.Handler(), nil)
	if rec.Code != http.StatusOK || body.Status != health.StatusOK {
		t.Fatalf("GET /healthz = %d status=%q, want 200 ok; body=%s", rec.Code, body.Status, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "<html") {
		t.Fatalf("health endpoint served the UI instead of JSON")
	}
}

func TestHealthzRejectsOtherMethods(t *testing.T) {
	cfg := healthTestConfig(t)

	app, err := daggo.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("open app: %v", err)
	}
	t.Cleanup(func() { _ = app.Close() })

	req := httptest.NewRequest(http.MethodPost, "/healthz", nil)
	rec := httptest.NewRecorder()
	app.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST /healthz = %d, want 405", rec.Code)
	}
}

func TestHealthzReturns503WhenDatabaseIsUnreachable(t *testing.T) {
	cfg := healthTestConfig(t)
	cfg.Admin.SecretKey = "health-test-secret"

	app, err := daggo.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("open app: %v", err)
	}
	t.Cleanup(func() { _ = app.Close() })
	handler := app.Handler()

	if rec, _ := getHealth(t, handler, nil); rec.Code != http.StatusOK {
		t.Fatalf("GET /healthz before the outage = %d, want 200", rec.Code)
	}

	if err := app.Deps().Pool.Close(); err != nil {
		t.Fatalf("close pool: %v", err)
	}

	rec, body := getHealth(t, handler, nil)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("GET /healthz with the database down = %d, want 503; body=%s", rec.Code, rec.Body.String())
	}
	if body.Status != health.StatusUnhealthy {
		t.Fatalf("status = %q, want %q", body.Status, health.StatusUnhealthy)
	}
	if body.Database.Reachable {
		t.Fatalf("expected database to be reported unreachable")
	}
}

func TestHealthzReturns503WhenSchedulerIsEnabledButNotAlive(t *testing.T) {
	cfg := healthTestConfig(t)

	queries, pool, err := db.OpenRuntime(context.Background(), cfg.Database)
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })

	runtimeCtx, stopRuntime := context.WithCancel(context.Background())
	defer stopRuntime()
	handler, _, err := daggo.NewRouterWithDeps(runtimeCtx, cfg, queries, pool)
	if err != nil {
		t.Fatalf("build router: %v", err)
	}
	waitForSchedulerTick(t, handler)
	if rec, _ := getHealth(t, handler, nil); rec.Code != http.StatusOK {
		t.Fatalf("GET /healthz with a running scheduler = %d, want 200", rec.Code)
	}

	// Ending the runtime context stops the scheduler loop while the
	// database stays reachable.
	stopRuntime()

	deadline := time.Now().Add(5 * time.Second)
	var rec *httptest.ResponseRecorder
	var body health.Response
	for time.Now().Before(deadline) {
		rec, body = getHealth(t, handler, nil)
		if rec.Code == http.StatusServiceUnavailable {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("GET /healthz with a stopped scheduler = %d, want 503; body=%s", rec.Code, rec.Body.String())
	}
	if !body.Database.Reachable {
		t.Fatalf("expected database to stay reachable")
	}
	if !body.Scheduler.Enabled || body.Scheduler.Alive || body.Scheduler.State != dag.SchedulerStateStopped {
		t.Fatalf("expected an enabled, stopped scheduler, got %+v", body.Scheduler)
	}
	if body.Status != health.StatusUnhealthy {
		t.Fatalf("status = %q, want %q", body.Status, health.StatusUnhealthy)
	}
}

func TestHealthzReturns200WhenSchedulerIsDisabled(t *testing.T) {
	cfg := healthTestConfig(t)
	cfg.Scheduler.Enabled = false

	app, err := daggo.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("open app: %v", err)
	}
	t.Cleanup(func() { _ = app.Close() })

	rec, body := getHealth(t, app.Handler(), nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /healthz with the scheduler disabled = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if body.Scheduler.Enabled || body.Scheduler.Alive || body.Scheduler.State != health.SchedulerStateDisabled {
		t.Fatalf("expected a disabled scheduler, got %+v", body.Scheduler)
	}
}

func TestHealthzReports200AndDrainingWhileDraining(t *testing.T) {
	cfg := healthTestConfig(t)

	queries, pool, err := db.OpenRuntime(context.Background(), cfg.Database)
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })

	runtimeCtx, stopRuntime := context.WithCancel(context.Background())
	defer stopRuntime()
	handler, application, err := daggo.NewRouterWithDeps(runtimeCtx, cfg, queries, pool)
	if err != nil {
		t.Fatalf("build router: %v", err)
	}
	waitForSchedulerTick(t, handler)

	application.DeployLock.BeginDrain("test drain")

	rec, body := getHealth(t, handler, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /healthz while draining = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if !body.Draining || body.Status != health.StatusDraining {
		t.Fatalf("expected a draining status, got %+v", body)
	}
}
