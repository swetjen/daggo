package dag

import (
	"context"
	"database/sql"
	"log/slog"
	"time"

	"github.com/swetjen/daggo/db"
)

const (
	defaultRunRetentionInterval  = 24 * time.Hour
	defaultRunRetentionBatchSize = 1000
)

type RunRetentionOptions struct {
	RunDays   int
	Interval  time.Duration
	BatchSize int64
}

type RunRetentionResult struct {
	RunDays           int
	Cutoff            string
	DeletedRuns       int64
	DeletedQueueItems int64
}

type RunRetention struct {
	queries   db.Store
	pool      *sql.DB
	runDays   int
	interval  time.Duration
	batchSize int64
	nowFn     func() time.Time
}

func NewRunRetention(queries db.Store, pool *sql.DB, opts RunRetentionOptions) *RunRetention {
	interval := opts.Interval
	if interval <= 0 {
		interval = defaultRunRetentionInterval
	}
	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = defaultRunRetentionBatchSize
	}
	return &RunRetention{
		queries:   queries,
		pool:      pool,
		runDays:   opts.RunDays,
		interval:  interval,
		batchSize: batchSize,
		nowFn:     time.Now,
	}
}

func (r *RunRetention) Enabled() bool {
	return r != nil && r.runDays > 0 && r.queries != nil && r.pool != nil
}

func (r *RunRetention) RunDays() int {
	if r == nil {
		return 0
	}
	return r.runDays
}

func (r *RunRetention) Interval() time.Duration {
	if r == nil || r.interval <= 0 {
		return defaultRunRetentionInterval
	}
	return r.interval
}

func (r *RunRetention) Start(ctx context.Context) {
	if !r.Enabled() {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	go r.loop(ctx)
}

func (r *RunRetention) loop(ctx context.Context) {
	r.logResult(r.RunOnce(ctx))

	ticker := time.NewTicker(r.Interval())
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.logResult(r.RunOnce(ctx))
		}
	}
}

func (r *RunRetention) RunOnce(ctx context.Context) (RunRetentionResult, error) {
	if !r.Enabled() {
		return RunRetentionResult{}, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	cutoff := r.nowFn().UTC().AddDate(0, 0, -r.runDays).Format(time.RFC3339Nano)
	result := RunRetentionResult{
		RunDays: r.runDays,
		Cutoff:  cutoff,
	}

	for {
		tx, err := r.pool.BeginTx(ctx, nil)
		if err != nil {
			return result, err
		}
		store := db.WithTx(r.queries, tx)

		runIDs, err := store.RunGetManyForRetentionPurge(ctx, db.RunGetManyForRetentionPurgeParams{
			CompletedAt: cutoff,
			Limit:       r.batchSize,
		})
		if err != nil {
			_ = tx.Rollback()
			return result, err
		}

		for _, runID := range runIDs {
			if err := store.RunDeleteByID(ctx, runID); err != nil {
				_ = tx.Rollback()
				return result, err
			}
		}

		queueItemIDs, err := store.QueueItemGetManyForRetentionPurge(ctx, db.QueueItemGetManyForRetentionPurgeParams{
			CompletedAt: cutoff,
			Limit:       r.batchSize,
		})
		if err != nil {
			_ = tx.Rollback()
			return result, err
		}

		for _, queueItemID := range queueItemIDs {
			if err := store.QueueItemDeleteByID(ctx, queueItemID); err != nil {
				_ = tx.Rollback()
				return result, err
			}
		}

		if len(runIDs) == 0 && len(queueItemIDs) == 0 {
			_ = tx.Rollback()
			return result, nil
		}

		if err := tx.Commit(); err != nil {
			return result, err
		}

		result.DeletedRuns += int64(len(runIDs))
		result.DeletedQueueItems += int64(len(queueItemIDs))
	}
}

func (r *RunRetention) logResult(result RunRetentionResult, err error) {
	if err != nil {
		slog.Error("daggo: run retention purge failed", "error", err)
		return
	}
	if result.DeletedRuns == 0 && result.DeletedQueueItems == 0 {
		return
	}
	slog.Info(
		"daggo: run retention purge completed",
		"run_days", result.RunDays,
		"cutoff", result.Cutoff,
		"deleted_runs", result.DeletedRuns,
		"deleted_queue_items", result.DeletedQueueItems,
	)
}
