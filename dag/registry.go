package dag

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/swetjen/daggo/db"
)

type Registry struct {
	mu                    sync.RWMutex
	jobs                  map[string]JobDefinition
	schedulingPausedByJob map[string]bool
}

func NewRegistry() *Registry {
	return &Registry{
		jobs:                  make(map[string]JobDefinition),
		schedulingPausedByJob: make(map[string]bool),
	}
}

func (r *Registry) MustRegister(job JobDefinition) {
	if err := r.Register(job); err != nil {
		panic(err)
	}
}

func (r *Registry) Register(job JobDefinition) error {
	if r == nil {
		return errors.New("registry is nil")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if job.Key == "" {
		return errors.New("job key is required")
	}
	if _, exists := r.jobs[job.Key]; exists {
		return fmt.Errorf("job %q already registered", job.Key)
	}
	if len(job.Steps) == 0 {
		return fmt.Errorf("job %q has no steps", job.Key)
	}
	if err := validateJob(job); err != nil {
		return err
	}
	if job.DefaultParamsJSON == "" {
		job.DefaultParamsJSON = "{}"
	}
	r.jobs[job.Key] = job
	return nil
}

func (r *Registry) JobByKey(key string) (JobDefinition, bool) {
	if r == nil {
		return JobDefinition{}, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	job, ok := r.jobs[key]
	return job, ok
}

func (r *Registry) Jobs() []JobDefinition {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	keys := make([]string, 0, len(r.jobs))
	for key := range r.jobs {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]JobDefinition, 0, len(keys))
	for _, key := range keys {
		out = append(out, r.jobs[key])
	}
	return out
}

func (r *Registry) JobSchedulingPaused(key string) (bool, bool) {
	if r == nil {
		return false, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	trimmed := strings.TrimSpace(key)
	if trimmed == "" {
		return false, false
	}
	if _, ok := r.jobs[trimmed]; !ok {
		return false, false
	}
	return r.schedulingPausedByJob[trimmed], true
}

func (r *Registry) SetJobSchedulingPaused(key string, paused bool) (JobDefinition, error) {
	if r == nil {
		return JobDefinition{}, errors.New("registry is nil")
	}
	trimmed := strings.TrimSpace(key)
	if trimmed == "" {
		return JobDefinition{}, errors.New("job key is required")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	job, ok := r.jobs[trimmed]
	if !ok {
		return JobDefinition{}, fmt.Errorf("job %q not found", trimmed)
	}
	if paused {
		r.schedulingPausedByJob[trimmed] = true
	} else {
		delete(r.schedulingPausedByJob, trimmed)
	}
	return job, nil
}

func (r *Registry) SyncToDB(ctx context.Context, queries db.Store, pool *sql.DB) error {
	if r == nil {
		return errors.New("registry is nil")
	}
	if queries == nil || pool == nil {
		return errors.New("db is nil")
	}

	for _, job := range r.Jobs() {
		tx, err := pool.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		txQueries := db.WithTx(queries, tx)

		jobRow, err := txQueries.JobUpsert(ctx, db.JobUpsertParams{
			JobKey:            job.Key,
			DisplayName:       job.DisplayName,
			Description:       job.Description,
			DefaultParamsJson: job.DefaultParamsJSON,
		})
		if err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("upsert job %s: %w", job.Key, err)
		}

		if err := txQueries.JobNodeDeleteByJobID(ctx, jobRow.ID); err != nil {
			_ = tx.Rollback()
			return err
		}
		if err := txQueries.JobEdgeDeleteByJobID(ctx, jobRow.ID); err != nil {
			_ = tx.Rollback()
			return err
		}

		for idx, step := range job.Steps {
			metadata := map[string]any{
				"depends_on": step.DependsOn,
				"bindings":   step.Bindings,
			}
			if step.partitionDefinition != nil {
				metadata["partition"] = map[string]any{
					"kind":   step.partitionDefinition.Kind(),
					"assets": cloneStrings(step.partitionAssets),
				}
			}
			metaBytes, err := json.Marshal(metadata)
			if err != nil {
				_ = tx.Rollback()
				return err
			}
			_, err = txQueries.JobNodeCreate(ctx, db.JobNodeCreateParams{
				JobID:        jobRow.ID,
				StepKey:      step.Key,
				DisplayName:  step.DisplayName,
				Description:  step.Description,
				Kind:         "op",
				MetadataJson: string(metaBytes),
				SortIndex:    int64(idx),
			})
			if err != nil {
				_ = tx.Rollback()
				return fmt.Errorf("create node %s/%s: %w", job.Key, step.Key, err)
			}
			for _, dep := range step.DependsOn {
				if _, err := txQueries.JobEdgeCreate(ctx, db.JobEdgeCreateParams{
					JobID:       jobRow.ID,
					FromStepKey: dep,
					ToStepKey:   step.Key,
				}); err != nil {
					_ = tx.Rollback()
					return fmt.Errorf("create edge %s/%s<- %s: %w", job.Key, step.Key, dep, err)
				}
			}
		}

		if err := syncJobPartitions(ctx, txQueries, job, jobRow.ID, time.Now().UTC()); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("sync partitions for job %s: %w", job.Key, err)
		}

		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func validateJob(job JobDefinition) error {
	stepByKey := make(map[string]StepDefinition)
	for _, step := range job.Steps {
		if step.Key == "" {
			return fmt.Errorf("job %q has step with empty key", job.Key)
		}
		if !step.runValue.IsValid() {
			return fmt.Errorf("job %q step %q missing runtime", job.Key, step.Key)
		}
		if _, exists := stepByKey[step.Key]; exists {
			return fmt.Errorf("job %q has duplicate step %q", job.Key, step.Key)
		}
		stepByKey[step.Key] = step
	}
	for _, step := range job.Steps {
		for _, dep := range step.DependsOn {
			if _, exists := stepByKey[dep]; !exists {
				return fmt.Errorf("job %q step %q depends on unknown step %q", job.Key, step.Key, dep)
			}
		}
	}
	if _, err := topologicalOrder(job); err != nil {
		return fmt.Errorf("job %q invalid DAG: %w", job.Key, err)
	}
	scheduleByKey := make(map[string]struct{}, len(job.Schedules))
	for _, schedule := range job.Schedules {
		scheduleKey := strings.TrimSpace(schedule.Key)
		if scheduleKey == "" {
			return fmt.Errorf("job %q has schedule with empty key", job.Key)
		}
		if _, exists := scheduleByKey[scheduleKey]; exists {
			return fmt.Errorf("job %q has duplicate schedule key %q", job.Key, scheduleKey)
		}
		scheduleByKey[scheduleKey] = struct{}{}
	}
	return nil
}

func topologicalOrder(job JobDefinition) ([]StepDefinition, error) {
	stepByKey := make(map[string]StepDefinition, len(job.Steps))
	inDegree := make(map[string]int, len(job.Steps))
	dependents := make(map[string][]string, len(job.Steps))
	for _, step := range job.Steps {
		stepByKey[step.Key] = step
		inDegree[step.Key] = len(step.DependsOn)
		for _, dep := range step.DependsOn {
			dependents[dep] = append(dependents[dep], step.Key)
		}
	}

	queue := make([]string, 0)
	for _, step := range job.Steps {
		if inDegree[step.Key] == 0 {
			queue = append(queue, step.Key)
		}
	}
	sort.Strings(queue)

	ordered := make([]StepDefinition, 0, len(job.Steps))
	for len(queue) > 0 {
		key := queue[0]
		queue = queue[1:]
		ordered = append(ordered, stepByKey[key])
		for _, depKey := range dependents[key] {
			inDegree[depKey]--
			if inDegree[depKey] == 0 {
				queue = append(queue, depKey)
			}
		}
		sort.Strings(queue)
	}
	if len(ordered) != len(job.Steps) {
		return nil, errors.New("cycle detected")
	}
	return ordered, nil
}

func syncJobPartitions(ctx context.Context, store db.Store, job JobDefinition, jobID int64, now time.Time) error {
	existing, err := store.PartitionDefinitionGetManyByJobID(ctx, jobID)
	if err != nil {
		return err
	}

	partitionedSteps := make(map[string]StepDefinition)
	partitionFingerprint := ""

	for _, step := range job.Steps {
		if step.partitionDefinition == nil {
			continue
		}

		definitionKind, definitionJSON, err := serializePartitionDefinition(step.partitionDefinition)
		if err != nil {
			return fmt.Errorf("step %s definition serialization failed: %w", step.Key, err)
		}
		keys, err := step.partitionDefinition.PartitionKeys(ctx, now)
		if err != nil {
			return fmt.Errorf("step %s partition keys failed: %w", step.Key, err)
		}
		keys, err = normalizeOrderedKeys(keys, "partition key")
		if err != nil {
			return fmt.Errorf("step %s partition keys invalid: %w", step.Key, err)
		}

		currentFingerprint := strings.Join([]string{string(definitionKind), definitionJSON, strings.Join(keys, "\x1f")}, "|")
		if partitionFingerprint == "" {
			partitionFingerprint = currentFingerprint
		} else if partitionFingerprint != currentFingerprint {
			return fmt.Errorf("multiple partition domains found across ops in job %q; v1 supports a single shared partition domain per job", job.Key)
		}

		definitionRow, err := store.PartitionDefinitionUpsert(ctx, db.PartitionDefinitionUpsertParams{
			JobID:          jobID,
			TargetKind:     "op",
			TargetKey:      step.Key,
			DefinitionKind: string(definitionKind),
			DefinitionJson: definitionJSON,
			Enabled:        1,
		})
		if err != nil {
			return err
		}

		if err := store.PartitionKeyDeleteByDefinitionID(ctx, definitionRow.ID); err != nil {
			return err
		}
		for index, key := range keys {
			if _, err := store.PartitionKeyUpsert(ctx, db.PartitionKeyUpsertParams{
				PartitionDefinitionID: definitionRow.ID,
				PartitionKey:          key,
				SortIndex:             int64(index),
				IsActive:              1,
			}); err != nil {
				return err
			}
		}

		partitionedSteps[step.Key] = step
	}

	for _, definition := range existing {
		if definition.TargetKind != "op" {
			continue
		}
		if _, exists := partitionedSteps[definition.TargetKey]; exists {
			continue
		}
		if err := store.PartitionDefinitionDeleteByID(ctx, definition.ID); err != nil {
			return err
		}
	}

	return nil
}

func serializePartitionDefinition(definition PartitionDefinition) (PartitionDefinitionKind, string, error) {
	if definition == nil {
		return "", "", fmt.Errorf("partition definition is required")
	}

	switch typed := definition.(type) {
	case StaticPartitionDefinition:
		payload, err := json.Marshal(map[string]any{
			"keys": cloneStrings(typed.keys),
		})
		if err != nil {
			return "", "", err
		}
		return typed.Kind(), string(payload), nil
	case DynamicPartitionDefinition:
		payload, err := json.Marshal(map[string]any{
			"name": strings.TrimSpace(typed.Name),
		})
		if err != nil {
			return "", "", err
		}
		return typed.Kind(), string(payload), nil
	case TimeWindowPartitionDefinition:
		location := ""
		if typed.Location != nil {
			location = typed.Location.String()
		}
		payload, err := json.Marshal(map[string]any{
			"cadence":         typed.Cadence,
			"start":           typed.Start.UTC().Format(time.RFC3339Nano),
			"end_offset":      typed.EndOffset,
			"timezone":        location,
			"format":          typed.Format,
			"minute_interval": typed.MinuteInterval,
		})
		if err != nil {
			return "", "", err
		}
		return typed.Kind(), string(payload), nil
	case *MultiPartitionDefinition:
		if typed == nil {
			return "", "", fmt.Errorf("multi partition definition is nil")
		}
		dimensions := make(map[string]map[string]any, len(typed.dimensions))
		for _, name := range typed.dimensionNames {
			dimension := typed.dimensions[name]
			kind, encoded, err := serializePartitionDefinition(dimension)
			if err != nil {
				return "", "", err
			}
			dimensions[name] = map[string]any{
				"kind":   kind,
				"config": json.RawMessage(encoded),
			}
		}
		payload, err := json.Marshal(map[string]any{
			"dimensions": dimensions,
		})
		if err != nil {
			return "", "", err
		}
		return typed.Kind(), string(payload), nil
	case customPartitionDefinition:
		spec := typed.Spec()
		configJSON, err := customPartitionSpecJSON(spec)
		if err != nil {
			return "", "", err
		}
		return typed.Kind(), configJSON, nil
	default:
		return "", "", fmt.Errorf("unsupported partition definition type %T", definition)
	}
}
