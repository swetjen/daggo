package deps

import (
	"context"
	"database/sql"
	"time"

	"github.com/swetjen/daggo/config"
	"github.com/swetjen/daggo/dag"
	"github.com/swetjen/daggo/db"
	"github.com/swetjen/daggo/queue"
)

// Deps contains shared runtime dependencies for handlers.
type Deps struct {
	Config     config.Config
	Version    string
	DB         db.Store
	Pool       *sql.DB
	Registry   *dag.Registry
	Queues     *queue.Registry
	Executor   *dag.Executor
	Scheduler  *dag.Scheduler
	QueueRun   *queue.Runner
	DeployLock *dag.DeployLock
}

func New(ctx context.Context, cfg config.Config, queries db.Store, pool *sql.DB) (*Deps, error) {
	return NewWithDefinitions(ctx, cfg, queries, pool, nil, nil)
}

func NewWithRegistry(ctx context.Context, cfg config.Config, queries db.Store, pool *sql.DB, registry *dag.Registry) (*Deps, error) {
	return NewWithDefinitions(ctx, cfg, queries, pool, registry, nil)
}

func NewWithDefinitions(ctx context.Context, cfg config.Config, queries db.Store, pool *sql.DB, registry *dag.Registry, queues *queue.Registry) (*Deps, error) {
	cfg = cfg.Normalized()
	if registry == nil {
		registry = dag.NewRegistry()
	}
	if queues == nil {
		queues = queue.NewRegistry()
	}
	if err := registry.SyncToDB(ctx, queries, pool); err != nil {
		return nil, err
	}
	if err := queues.SyncToDB(ctx, queries, pool); err != nil {
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
	queueRunner := queue.NewRunner(queries, pool, queues, executor, deployLock)
	queueRunner.Start(ctx)
	return &Deps{
		Config:     cfg,
		DB:         queries,
		Pool:       pool,
		Registry:   registry,
		Queues:     queues,
		Executor:   executor,
		Scheduler:  scheduler,
		QueueRun:   queueRunner,
		DeployLock: deployLock,
	}, nil
}
