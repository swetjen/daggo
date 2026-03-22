package queue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/swetjen/daggo/dag"
	"github.com/swetjen/daggo/db"
)

type RunEnqueuer interface {
	EnqueueRun(runID int64)
}

type DeployDrainer interface {
	IsDraining() bool
}

type Runner struct {
	queries    db.Store
	pool       *sql.DB
	registry   *Registry
	enqueuer   RunEnqueuer
	deployLock DeployDrainer
	nowFn      func() time.Time
}

func NewRunner(queries db.Store, pool *sql.DB, registry *Registry, enqueuer RunEnqueuer, deployLock DeployDrainer) *Runner {
	return &Runner{
		queries:    queries,
		pool:       pool,
		registry:   registry,
		enqueuer:   enqueuer,
		deployLock: deployLock,
		nowFn:      time.Now,
	}
}

func (r *Runner) Start(ctx context.Context) {
	if r == nil || r.registry == nil {
		return
	}
	for _, definition := range r.registry.Queues() {
		definition := definition
		switch definition.LoadMode {
		case LoadModeStream:
			go r.runStreamLoader(ctx, definition)
		default:
			go r.runPollLoader(ctx, definition)
		}
	}
}

func (r *Runner) runPollLoader(ctx context.Context, definition Definition) {
	interval := definition.LoadPollEvery
	if interval <= 0 {
		interval = time.Second
	}
	r.runLoaderOnce(ctx, definition)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.runLoaderOnce(ctx, definition)
		}
	}
}

func (r *Runner) runStreamLoader(ctx context.Context, definition Definition) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if err := definition.Load(ctx, func(item runtimeLoadedItem) error {
			return r.processLoadedItem(ctx, definition, item)
		}); err != nil && !errors.Is(err, context.Canceled) {
			slog.Error("daggo: queue loader failed", "queue_key", definition.Key, "mode", definition.LoadMode, "err", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		return
	}
}

func (r *Runner) runLoaderOnce(ctx context.Context, definition Definition) {
	if err := definition.Load(ctx, func(item runtimeLoadedItem) error {
		return r.processLoadedItem(ctx, definition, item)
	}); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("daggo: queue poll failed", "queue_key", definition.Key, "mode", definition.LoadMode, "err", err)
	}
}

func (r *Runner) processLoadedItem(ctx context.Context, definition Definition, item runtimeLoadedItem) error {
	if r == nil || r.queries == nil || r.pool == nil {
		return fmt.Errorf("queue runtime is unavailable")
	}
	if r.deployLock != nil && r.deployLock.IsDraining() {
		return fmt.Errorf("deployment in progress; queue processing blocked")
	}
	partitionKey, err := definition.ResolvePartitionKey(item.Value)
	if err != nil {
		return err
	}
	payloadJSON, err := definition.MarshalPayloadJSON(item.Value)
	if err != nil {
		return err
	}

	var payload any
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		return fmt.Errorf("decode queue payload json: %w", err)
	}

	tx, err := r.pool.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	qtx := db.WithTx(r.queries, tx)

	queueRow, err := qtx.QueueGetByKey(ctx, definition.Key)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	now := r.nowFn().UTC()
	queuedAt := item.QueuedAt.UTC()
	if item.QueuedAt.IsZero() {
		queuedAt = now
	}
	queueItem, err := qtx.QueueItemCreate(ctx, db.QueueItemCreateParams{
		QueueID:      queueRow.ID,
		QueueItemKey: fmt.Sprintf("qit_%d", now.UnixNano()),
		PartitionKey: partitionKey,
		Status:       "queued",
		ExternalKey:  strings.TrimSpace(item.ExternalKey),
		PayloadJson:  payloadJSON,
		ErrorMessage: "",
		QueuedAt:     queuedAt.Format(time.RFC3339Nano),
		StartedAt:    "",
		CompletedAt:  "",
	})
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	runIDs := make([]int64, 0, len(definition.jobs))
	for _, job := range definition.Jobs() {
		jobRow, err := qtx.JobGetByKey(ctx, job.Key)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
		paramsBytes, err := json.Marshal(dag.QueueRunParamsMap(dag.RunQueueMeta{
			QueueKey:     definition.Key,
			QueueItemID:  queueItem.ID,
			PartitionKey: partitionKey,
			ExternalKey:  strings.TrimSpace(item.ExternalKey),
			Payload:      payload,
		}))
		if err != nil {
			_ = tx.Rollback()
			return err
		}
		run, _, err := dag.InsertQueuedRun(ctx, qtx, dag.RunInsertInput{
			JobID:       jobRow.ID,
			TriggeredBy: "queue:" + definition.Key,
			ParamsJSON:  string(paramsBytes),
			QueuedAt:    queuedAt.Format(time.RFC3339Nano),
			EventPayload: map[string]any{
				"queue_key":     definition.Key,
				"queue_item_id": queueItem.ID,
				"partition_key": partitionKey,
				"external_key":  strings.TrimSpace(item.ExternalKey),
			},
		})
		if err != nil {
			_ = tx.Rollback()
			return err
		}
		if _, err := qtx.QueueItemRunCreate(ctx, db.QueueItemRunCreateParams{
			QueueItemID: queueItem.ID,
			JobID:       jobRow.ID,
			RunID:       run.ID,
		}); err != nil {
			_ = tx.Rollback()
			return err
		}
		runIDs = append(runIDs, run.ID)
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	for _, runID := range runIDs {
		if r.enqueuer != nil {
			r.enqueuer.EnqueueRun(runID)
		}
	}
	return nil
}
