package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	api "daggo"
	"daggo/config"
	"daggo/dag"
	"daggo/db"
	"daggo/deps"
	"daggo/jobs"
)

func main() {
	handled, err := maybeRunWorker()
	if handled {
		if err != nil {
			log.Fatal(err)
		}
		return
	}

	server, cleanup, err := RunServer()
	if err != nil {
		log.Fatal(err)
	}
	if cleanup != nil {
		defer cleanup()
	}
	if err := ListenAndServe(server); err != nil {
		log.Fatal(err)
	}
}

func maybeRunWorker() (bool, error) {
	if len(os.Args) < 2 {
		return false, nil
	}
	if os.Args[1] != "worker" {
		return false, nil
	}

	flagSet := flag.NewFlagSet("worker", flag.ContinueOnError)
	runID := flagSet.Int64("run-id", 0, "run ID to execute")
	if err := flagSet.Parse(os.Args[2:]); err != nil {
		return true, err
	}
	if *runID <= 0 {
		return true, fmt.Errorf("--run-id must be greater than zero")
	}
	return true, runWorker(*runID)
}

func runWorker(runID int64) error {
	runtimeCtx := context.Background()
	cfg := config.Load()
	queries, pool, err := db.Open(runtimeCtx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	registry := jobs.DefaultRegistry()
	executor := dag.NewExecutor(queries, pool, registry, 1)
	executor.SetExecutionMode(dag.ExecutionModeInProcess)
	executor.SetRunMaxConcurrentRuns(1)
	executor.SetRunMaxConcurrentSteps(cfg.RunMaxConcurrentSteps)

	log.Printf("daggo worker starting run_id=%d", runID)
	if err := executor.ExecuteRun(runtimeCtx, runID); err != nil {
		return err
	}
	log.Printf("daggo worker completed run_id=%d", runID)
	return nil
}

func RunServer() (*http.Server, func(), error) {
	runtimeCtx, cancel := context.WithCancel(context.Background())
	cfg := config.Load()
	queries, pool, err := db.Open(runtimeCtx, cfg.DatabaseURL)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	cleanup := func() {
		cancel()
		pool.Close()
	}
	handler, appDeps, err := api.NewRouterWithDeps(runtimeCtx, cfg, queries, pool)
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	server := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: handler,
	}
	startDeployDrainMonitor(runtimeCtx, server, appDeps, cancel)
	return server, cleanup, nil
}

func ListenAndServe(server *http.Server) error {
	if server == nil {
		return fmt.Errorf("server is nil")
	}
	fmt.Println("DAGGO server listening on " + server.Addr)
	err := server.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
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
