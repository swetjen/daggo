package dag

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/swetjen/daggo/db"
)

type Registry struct {
	jobs map[string]JobDefinition
}

func NewRegistry() *Registry {
	return &Registry{jobs: make(map[string]JobDefinition)}
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
	job, ok := r.jobs[key]
	return job, ok
}

func (r *Registry) Jobs() []JobDefinition {
	if r == nil {
		return nil
	}
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
			metaBytes, err := json.Marshal(map[string]any{
				"depends_on": step.DependsOn,
				"bindings":   step.Bindings,
			})
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
