package deps

import (
	"context"
	"database/sql"
	"time"

	"github.com/swetjen/daggo/config"
	"github.com/swetjen/daggo/dag"
	"github.com/swetjen/daggo/db"
)

// Deps contains shared runtime dependencies for handlers.
type Deps struct {
	Config     config.Config
	DB         db.Store
	Pool       *sql.DB
	Registry   *dag.Registry
	Executor   *dag.Executor
	Scheduler  *dag.Scheduler
	DeployLock *dag.DeployLock
}

func New(ctx context.Context, cfg config.Config, queries db.Store, pool *sql.DB) (*Deps, error) {
	return NewWithRegistry(ctx, cfg, queries, pool, nil)
}

func NewWithRegistry(ctx context.Context, cfg config.Config, queries db.Store, pool *sql.DB, registry *dag.Registry) (*Deps, error) {
	cfg = cfg.Normalized()
	if registry == nil {
		registry = dag.NewRegistry()
	}
	if err := registry.SyncToDB(ctx, queries, pool); err != nil {
		return nil, err
	}
	deployLock := dag.NewDeployLock(
		cfg.Deploy.LockPath,
		time.Duration(cfg.Deploy.PollSeconds)*time.Second,
		time.Duration(cfg.Deploy.DrainGraceSeconds)*time.Second,
	)
	if err := dag.EnsureDeployLockDir(cfg.Deploy.LockPath); err != nil {
		return nil, err
	}
	executor := dag.NewExecutor(queries, pool, registry, cfg.Execution.QueueSize)
	executor.SetExecutionMode(cfg.Execution.Mode)
	executor.SetRunMaxConcurrentRuns(cfg.Execution.MaxConcurrentRuns)
	executor.SetRunMaxConcurrentSteps(cfg.Execution.MaxConcurrentSteps)
	scheduler := dag.NewScheduler(queries, pool, executor, dag.SchedulerOptions{
		SchedulerKey:  cfg.Scheduler.Key,
		TickInterval:  time.Duration(cfg.Scheduler.TickSeconds) * time.Second,
		MaxDuePerTick: cfg.Scheduler.MaxDuePerTick,
		DeployLock:    deployLock,
		Registry:      registry,
	})
	if cfg.Scheduler.Enabled {
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
