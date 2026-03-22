package queue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/swetjen/daggo/db"
)

type Registry struct {
	mu     sync.RWMutex
	queues map[string]Definition
}

func NewRegistry() *Registry {
	return &Registry{queues: make(map[string]Definition)}
}

func (r *Registry) Register(definition Definition) error {
	if r == nil {
		return errors.New("queue registry is nil")
	}
	if strings.TrimSpace(definition.Key) == "" {
		return errors.New("queue key is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.queues[definition.Key]; exists {
		return fmt.Errorf("queue %q already registered", definition.Key)
	}
	r.queues[definition.Key] = definition
	return nil
}

func (r *Registry) QueueByKey(key string) (Definition, bool) {
	if r == nil {
		return Definition{}, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	definition, ok := r.queues[strings.TrimSpace(key)]
	return definition, ok
}

func (r *Registry) Queues() []Definition {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	keys := make([]string, 0, len(r.queues))
	for key := range r.queues {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]Definition, 0, len(keys))
	for _, key := range keys {
		out = append(out, r.queues[key])
	}
	return out
}

func (r *Registry) SyncToDB(ctx context.Context, queries db.Store, pool *sql.DB) error {
	if r == nil {
		return errors.New("queue registry is nil")
	}
	if queries == nil || pool == nil {
		return errors.New("db is nil")
	}
	for _, definition := range r.Queues() {
		tx, err := pool.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		qtx := db.WithTx(queries, tx)
		row, err := qtx.QueueUpsert(ctx, db.QueueUpsertParams{
			QueueKey:             definition.Key,
			DisplayName:          definition.DisplayName,
			Description:          definition.Description,
			RoutePath:            definition.RoutePath,
			LoadMode:             string(definition.LoadMode),
			LoadPollEverySeconds: int64(definition.LoadPollEvery / time.Second),
		})
		if err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("upsert queue %s: %w", definition.Key, err)
		}
		if err := qtx.QueueJobDeleteByQueueID(ctx, row.ID); err != nil {
			_ = tx.Rollback()
			return err
		}
		for idx, job := range definition.Jobs() {
			jobRow, err := qtx.JobGetByKey(ctx, job.Key)
			if err != nil {
				_ = tx.Rollback()
				return fmt.Errorf("load queue job %s/%s: %w", definition.Key, job.Key, err)
			}
			if _, err := qtx.QueueJobCreate(ctx, db.QueueJobCreateParams{
				QueueID:    row.ID,
				JobID:      jobRow.ID,
				SortIndex:  int64(idx),
			}); err != nil {
				_ = tx.Rollback()
				return fmt.Errorf("create queue job %s/%s: %w", definition.Key, job.Key, err)
			}
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
