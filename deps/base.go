package deps

import (
	"context"
	"database/sql"
	"time"

	"daggo/config"
	"daggo/dag"
	"daggo/db"
	"daggo/jobs"
)

// Deps contains shared runtime dependencies for handlers.
type Deps struct {
	Config     config.Config
	DB         *db.Queries
	Pool       *sql.DB
	Registry   *dag.Registry
	Executor   *dag.Executor
	Scheduler  *dag.Scheduler
	DeployLock *dag.DeployLock
}

func New(ctx context.Context, cfg config.Config, queries *db.Queries, pool *sql.DB) (*Deps, error) {
	registry := jobs.DefaultRegistry()
	if err := registry.SyncToDB(ctx, queries, pool); err != nil {
		return nil, err
	}
	deployLock := dag.NewDeployLock(
		cfg.DeployLockPath,
		time.Duration(cfg.DeployLockPollSeconds)*time.Second,
		time.Duration(cfg.DeployDrainGraceSeconds)*time.Second,
	)
	if err := dag.EnsureDeployLockDir(cfg.DeployLockPath); err != nil {
		return nil, err
	}
	executor := dag.NewExecutor(queries, pool, registry, cfg.RunQueueSize)
	executor.SetExecutionMode(cfg.RunExecutionMode)
	executor.SetRunMaxConcurrentRuns(cfg.RunMaxConcurrentRuns)
	executor.SetRunMaxConcurrentSteps(cfg.RunMaxConcurrentSteps)
	scheduler := dag.NewScheduler(queries, pool, executor, dag.SchedulerOptions{
		SchedulerKey:  cfg.SchedulerKey,
		TickInterval:  time.Duration(cfg.SchedulerTickSeconds) * time.Second,
		MaxDuePerTick: cfg.SchedulerMaxDuePerTick,
		DeployLock:    deployLock,
	})
	if cfg.SchedulerEnabled {
		scheduler.Start(ctx)
	}
	return &Deps{
		Config:     cfg,
		DB:         queries,
		Pool:       pool,
		Registry:   registry,
		Executor:   executor,
		Scheduler:  scheduler,
		DeployLock: deployLock,
	}, nil
}
