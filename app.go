package daggo

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/swetjen/daggo/config"
	"github.com/swetjen/daggo/dag"
	"github.com/swetjen/daggo/db"
	"github.com/swetjen/daggo/deps"
	"github.com/swetjen/daggo/queue"
)

const (
	internalWorkerCommand = "daggo-worker"

	// maxDrainCheckInterval bounds how long a finished drain can go
	// unnoticed once it has started.
	maxDrainCheckInterval = 200 * time.Millisecond
	// interruptRunsTimeout bounds recording interrupted runs at shutdown.
	interruptRunsTimeout = 15 * time.Second
	// drainFinishTimeout bounds how long Run waits, after the listener has
	// closed, for the drain to finish shutting the server down.
	drainFinishTimeout = 10 * time.Second
)

type App struct {
	cfg      config.Config
	runtime  context.Context
	cancel   context.CancelFunc
	registry *dag.Registry
	queues   *queue.Registry
	queries  db.Store
	pool     *sql.DB
	handler  http.Handler
	server   *http.Server
	deps     *deps.Deps

	// drainDone is closed when the drain monitor has returned. closed is
	// closed by Close and ends the monitor.
	drainDone chan struct{}
	closed    chan struct{}

	closeOnce sync.Once
	closeErr  error
}

func Run(ctx context.Context, cfg Config, jobs ...dag.JobDefinition) error {
	registry, err := registryFromJobs(jobs...)
	if err != nil {
		return err
	}
	return runWithDefinitions(ctx, cfg, registry, nil)
}

func RunRegistry(ctx context.Context, cfg Config, registry *dag.Registry) error {
	cloned, err := cloneRegistry(registry)
	if err != nil {
		return err
	}
	return runWithDefinitions(ctx, cfg, cloned, nil)
}

func runWithDefinitions(ctx context.Context, cfg Config, registry *dag.Registry, queues *queue.Registry) error {
	cfg = cfg.Normalized()
	if err := cfg.Validate(); err != nil {
		return err
	}

	process, err := CurrentProcess()
	if err != nil {
		return err
	}
	if process.Mode == ProcessModeWorker {
		return runWorker(ctx, cfg, registry, process.RunID)
	}

	app, err := openWithDefinitions(ctx, cfg, registry, queues)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := app.Close(); closeErr != nil {
			log.Printf("daggo: close failed: %v", closeErr)
		}
	}()
	stopSignals := app.handleShutdownSignals()
	defer stopSignals()

	err = app.ListenAndServe()
	// The listener closes as soon as shutdown begins. Let the drain finish
	// shutting down before resources are closed and the process exits.
	app.waitForDrain(drainFinishTimeout)
	return err
}

func Open(ctx context.Context, cfg Config, jobs ...dag.JobDefinition) (*App, error) {
	registry, err := registryFromJobs(jobs...)
	if err != nil {
		return nil, err
	}
	return openWithDefinitions(ctx, cfg, registry, nil)
}

func OpenRegistry(ctx context.Context, cfg Config, registry *dag.Registry) (*App, error) {
	cloned, err := cloneRegistry(registry)
	if err != nil {
		return nil, err
	}
	return openWithDefinitions(ctx, cfg, cloned, nil)
}

func openWithDefinitions(ctx context.Context, cfg Config, registry *dag.Registry, queues *queue.Registry) (*App, error) {
	cfg = cfg.Normalized()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}

	runtimeCtx, cancel := context.WithCancel(ctx)
	queries, pool, err := db.OpenRuntime(runtimeCtx, cfg.Database)
	if err != nil {
		cancel()
		return nil, err
	}

	handler, application, err := NewRouterWithDepsAndDefinitions(runtimeCtx, cfg, queries, pool, registry, queues)
	if err != nil {
		cancel()
		_ = pool.Close()
		return nil, err
	}

	app := &App{
		cfg:      cfg,
		runtime:  runtimeCtx,
		cancel:   cancel,
		registry: registry,
		queues:   queues,
		queries:  queries,
		pool:     pool,
		handler:  handler,
		deps:     application,
		server: &http.Server{
			Addr:    cfg.ListenAddr(),
			Handler: handler,
		},
		drainDone: make(chan struct{}),
		closed:    make(chan struct{}),
	}
	app.startDrainMonitor()
	return app, nil
}

func (a *App) Config() Config {
	if a == nil {
		return DefaultConfig()
	}
	return a.cfg
}

func (a *App) Handler() http.Handler {
	if a == nil {
		return nil
	}
	return a.handler
}

func (a *App) Server() *http.Server {
	if a == nil {
		return nil
	}
	return a.server
}

func (a *App) Registry() *dag.Registry {
	if a == nil {
		return nil
	}
	return a.registry
}

func (a *App) QueueRegistry() *queue.Registry {
	if a == nil {
		return nil
	}
	return a.queues
}

func (a *App) Deps() *deps.Deps {
	if a == nil {
		return nil
	}
	return a.deps
}

func (a *App) ListenAndServe() error {
	if a == nil || a.server == nil {
		return fmt.Errorf("server is nil")
	}
	fmt.Print(startupBanner(a.cfg, a.server.Addr))
	err := a.server.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func (a *App) Shutdown(ctx context.Context) error {
	if a == nil {
		return nil
	}
	if a.cancel != nil {
		a.cancel()
	}
	if a.server == nil {
		return a.Close()
	}
	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
	}
	err := a.server.Shutdown(ctx)
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		_ = a.server.Close()
	}
	closeErr := a.Close()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		if closeErr != nil {
			return fmt.Errorf("shutdown server: %w; close resources: %v", err, closeErr)
		}
		return err
	}
	return closeErr
}

func (a *App) Close() error {
	if a == nil {
		return nil
	}
	a.closeOnce.Do(func() {
		if a.closed != nil {
			close(a.closed)
		}
		if a.cancel != nil {
			a.cancel()
		}
		if a.pool != nil {
			a.closeErr = a.pool.Close()
		}
	})
	return a.closeErr
}

func registryFromJobs(jobs ...dag.JobDefinition) (*dag.Registry, error) {
	registry := dag.NewRegistry()
	for _, job := range jobs {
		if err := registry.Register(job); err != nil {
			return nil, err
		}
	}
	return registry, nil
}

func cloneRegistry(source *dag.Registry) (*dag.Registry, error) {
	registry := dag.NewRegistry()
	if source == nil {
		return registry, nil
	}
	for _, job := range source.Jobs() {
		if err := registry.Register(job); err != nil {
			return nil, err
		}
	}
	return registry, nil
}

func cloneQueueRegistry(source *queue.Registry) (*queue.Registry, error) {
	registry := queue.NewRegistry()
	if source == nil {
		return registry, nil
	}
	for _, definition := range source.Queues() {
		if err := registry.Register(definition); err != nil {
			return nil, err
		}
	}
	return registry, nil
}

func maybeParseWorkerCommand(args []string) (bool, int64, error) {
	if len(args) == 0 {
		return false, 0, nil
	}
	command := strings.TrimSpace(args[0])
	if command != internalWorkerCommand {
		return false, 0, nil
	}

	flagSet := flag.NewFlagSet(command, flag.ContinueOnError)
	flagSet.SetOutput(io.Discard)
	runID := flagSet.Int64("run-id", 0, "run ID to execute")
	if err := flagSet.Parse(args[1:]); err != nil {
		return true, 0, err
	}
	if *runID <= 0 {
		return true, 0, fmt.Errorf("--run-id must be greater than zero")
	}
	return true, *runID, nil
}

func runWorker(ctx context.Context, cfg config.Config, registry *dag.Registry, runID int64) error {
	if ctx == nil {
		ctx = context.Background()
	}
	cfg = cfg.Normalized()
	queries, pool, err := db.OpenRuntime(ctx, cfg.Database)
	if err != nil {
		return err
	}
	defer pool.Close()

	executor := dag.NewExecutor(queries, pool, registry, 1)
	executor.SetExecutionMode(dag.ExecutionModeInProcess)
	executor.SetRunMaxConcurrentRuns(1)
	executor.SetRunMaxConcurrentSteps(cfg.Execution.MaxConcurrentSteps)

	log.Printf("daggo worker starting run_id=%d", runID)
	if err := executor.ExecuteRun(ctx, runID); err != nil {
		return err
	}
	log.Printf("daggo worker completed run_id=%d", runID)
	return nil
}

// handleShutdownSignals makes SIGINT and SIGTERM start a graceful drain: new
// runs are blocked, the scheduler stops creating runs, in-flight runs get the
// deploy drain grace period to finish, and the server then shuts down. A
// second signal ends the grace period immediately. The returned function
// restores default signal handling.
func (a *App) handleShutdownSignals() func() {
	if a == nil || a.deps == nil || a.deps.DeployLock == nil {
		return func() {}
	}
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	done := make(chan struct{})
	go func() {
		received := 0
		for {
			select {
			case <-done:
				return
			case sig := <-signals:
				received++
				reason := "signal " + sig.String()
				if received == 1 {
					log.Printf(
						"daggo: received %s; draining in-flight runs for up to %s (send the signal again to stop now)",
						sig, a.deps.DeployLock.GracePeriod(),
					)
					a.deps.DeployLock.BeginDrain(reason)
					continue
				}
				log.Printf("daggo: received %s again; ending in-flight runs now", sig)
				a.deps.DeployLock.ForceExit(reason)
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() {
			signal.Stop(signals)
			close(done)
		})
	}
}

// waitForDrain waits for a drain that is in progress to finish shutting down.
// It returns immediately when no drain has started.
func (a *App) waitForDrain(timeout time.Duration) {
	if a == nil || a.drainDone == nil || a.deps == nil || a.deps.DeployLock == nil {
		return
	}
	if _, draining := a.deps.DeployLock.DrainStartedAt(); !draining {
		return
	}
	select {
	case <-a.drainDone:
	case <-time.After(timeout):
		log.Printf("daggo: timed out after %s waiting for the drain to finish", timeout)
	}
}

// startDrainMonitor watches for a drain, whether it starts from the deploy
// lock file or from a termination signal, and then shuts the server down:
// once the executor is idle, or once the grace period is over, in which case
// the runs still in flight are interrupted first.
func (a *App) startDrainMonitor() {
	if a == nil || a.drainDone == nil {
		return
	}
	if a.runtime == nil || a.server == nil || a.deps == nil || a.deps.DeployLock == nil || a.deps.Executor == nil {
		close(a.drainDone)
		return
	}
	go func() {
		defer close(a.drainDone)
		if !a.waitForDrainStart() {
			return
		}
		a.finishDrain()
	}()
}

// waitForDrainStart blocks until a drain begins. It reports false when the
// app is closed, or its context ends, without a drain having started.
func (a *App) waitForDrainStart() bool {
	lock := a.deps.DeployLock
	interval := lock.PollInterval()
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-a.closed:
			return false
		case <-lock.DrainStarted():
			return true
		case <-a.runtime.Done():
			// A drain that began together with the context ending still
			// has to run to completion.
			_, draining := lock.DrainStartedAt()
			return draining
		case <-ticker.C:
			if lock.IsDraining() {
				return true
			}
		}
	}
}

func (a *App) finishDrain() {
	lock := a.deps.DeployLock
	executor := a.deps.Executor

	interval := lock.PollInterval()
	if interval <= 0 || interval > maxDrainCheckInterval {
		interval = maxDrainCheckInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		if executor.IsIdle() {
			log.Printf("daggo: drain active (%s) and executor is idle; shutting down", lock.DrainReason())
			break
		}
		if lock.ShouldForceExit() {
			log.Printf(
				"daggo: drain grace period is over; interrupting in-flight runs active_runs=%d queue_depth=%d",
				executor.ActiveRuns(),
				executor.QueueDepth(),
			)
			ctx, stop := context.WithTimeout(context.Background(), interruptRunsTimeout)
			interrupted := executor.InterruptActiveRuns(ctx, dag.RunInterruptedReason)
			stop()
			log.Printf("daggo: interrupted %d run(s) for shutdown run_ids=%v", len(interrupted), interrupted)
			break
		}
		select {
		case <-a.closed:
			return
		case <-lock.ForceExitRequested():
		case <-ticker.C:
		}
	}
	shutdownServer(a.server, a.cancel)
}

func shutdownServer(server *http.Server, cancel context.CancelFunc) {
	if cancel != nil {
		cancel()
	}
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	if err := server.Shutdown(ctx); err != nil {
		log.Printf("daggo: graceful shutdown failed: %v", err)
		_ = server.Close()
	}
}

func startupBanner(cfg config.Config, addr string) string {
	baseURL := consoleBaseURL(addr)
	version := Version()
	if cfg.Normalized().DisableUI {
		return fmt.Sprintf(
			"\n[DAGGO]\nVersion:  %s\nRPC docs: %s/rpc/docs\nPress Ctrl+C to stop.\n\n",
			version,
			baseURL,
		)
	}
	return fmt.Sprintf(
		"\n[DAGGO]\nVersion:  %s\nUI:       %s/\nRPC docs: %s/rpc/docs\nPress Ctrl+C to stop.\n\n",
		version,
		baseURL,
		baseURL,
	)
}

func consoleBaseURL(addr string) string {
	trimmed := strings.TrimSpace(addr)
	if trimmed == "" {
		return "http://localhost:8000"
	}
	if strings.HasPrefix(trimmed, ":") {
		return "http://localhost" + trimmed
	}

	host, port, err := net.SplitHostPort(trimmed)
	if err == nil {
		host = strings.Trim(host, "[]")
		if host == "" || host == "0.0.0.0" || host == "::" {
			host = "localhost"
		}
		return "http://" + net.JoinHostPort(host, port)
	}
	return "http://" + trimmed
}
