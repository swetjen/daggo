package daggo

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/swetjen/daggo/config"
	"github.com/swetjen/daggo/dag"
	"github.com/swetjen/daggo/db"
	"github.com/swetjen/daggo/deps"
)

const (
	internalWorkerCommand = "daggo-worker"
)

type Option func(*appOptions) error

type appOptions struct {
	registry *dag.Registry
	jobs     []dag.JobDefinition
}

type App struct {
	cfg      config.Config
	runtime  context.Context
	cancel   context.CancelFunc
	registry *dag.Registry
	queries  db.Store
	pool     *sql.DB
	handler  http.Handler
	server   *http.Server
	deps     *deps.Deps

	closeOnce sync.Once
	closeErr  error
}

func WithJobs(jobs ...dag.JobDefinition) Option {
	return func(options *appOptions) error {
		options.jobs = append(options.jobs, jobs...)
		return nil
	}
}

func WithRegistry(registry *dag.Registry) Option {
	return func(options *appOptions) error {
		options.registry = registry
		return nil
	}
}

func Run(ctx context.Context, cfg Config, options ...Option) error {
	return Main(ctx, cfg, options...)
}

func Main(ctx context.Context, cfg Config, options ...Option) error {
	cfg = cfg.Normalized()
	if err := cfg.Validate(); err != nil {
		return err
	}

	registry, err := resolveRegistry(options...)
	if err != nil {
		return err
	}

	handled, runID, err := maybeParseWorkerCommand(os.Args[1:])
	if err != nil {
		return err
	}
	if handled {
		return runWorker(ctx, cfg, registry, runID)
	}

	app, err := NewApp(ctx, cfg, options...)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := app.Close(); closeErr != nil {
			log.Printf("daggo: close failed: %v", closeErr)
		}
	}()
	return app.ListenAndServe()
}

func NewApp(ctx context.Context, cfg Config, options ...Option) (*App, error) {
	cfg = cfg.Normalized()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}

	registry, err := resolveRegistry(options...)
	if err != nil {
		return nil, err
	}

	runtimeCtx, cancel := context.WithCancel(ctx)
	queries, pool, err := db.OpenRuntime(runtimeCtx, cfg.Database)
	if err != nil {
		cancel()
		return nil, err
	}

	handler, application, err := NewRouterWithDepsAndRegistry(runtimeCtx, cfg, queries, pool, registry)
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
		queries:  queries,
		pool:     pool,
		handler:  handler,
		deps:     application,
		server: &http.Server{
			Addr:    cfg.ListenAddr(),
			Handler: handler,
		},
	}
	startDeployDrainMonitor(runtimeCtx, app.server, application, cancel)
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
	fmt.Println("DAGGO server listening on " + a.server.Addr)
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
		if a.cancel != nil {
			a.cancel()
		}
		if a.pool != nil {
			a.closeErr = a.pool.Close()
		}
	})
	return a.closeErr
}

func resolveRegistry(options ...Option) (*dag.Registry, error) {
	var resolved appOptions
	for _, option := range options {
		if option == nil {
			continue
		}
		if err := option(&resolved); err != nil {
			return nil, err
		}
	}

	registry := dag.NewRegistry()
	if resolved.registry != nil {
		for _, job := range resolved.registry.Jobs() {
			if err := registry.Register(job); err != nil {
				return nil, err
			}
		}
	}
	for _, job := range resolved.jobs {
		if err := registry.Register(job); err != nil {
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

func startDeployDrainMonitor(runtimeCtx context.Context, server *http.Server, app *deps.Deps, cancel context.CancelFunc) {
	if runtimeCtx == nil || server == nil || app == nil || app.DeployLock == nil || app.Executor == nil {
		return
	}
	interval := app.DeployLock.PollInterval()
	if interval <= 0 {
		interval = time.Second
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-runtimeCtx.Done():
				return
			case <-ticker.C:
				if !app.DeployLock.IsDraining() {
					continue
				}
				if app.Executor.IsIdle() {
					log.Printf("daggo: deploy drain lock active and executor is idle; shutting down")
					shutdownServer(server, cancel)
					return
				}
				if app.DeployLock.ShouldForceExit() {
					log.Printf(
						"daggo: deploy drain grace period exceeded; forcing shutdown active_runs=%d queue_depth=%d",
						app.Executor.ActiveRuns(),
						app.Executor.QueueDepth(),
					)
					shutdownServer(server, cancel)
					return
				}
			}
		}
	}()
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
