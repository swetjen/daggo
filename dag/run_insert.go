package dag

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/swetjen/daggo/db"
)

type RunInsertInput struct {
	JobID         int64
	TriggeredBy   string
	ParamsJSON    string
	QueuedAt      string
	ParentRunID   int64
	RerunStepKey  string
	ErrorMessage  string
	EventPayload  map[string]any
}

func InsertQueuedRun(ctx context.Context, queries db.Store, in RunInsertInput) (db.Run, int, error) {
	nodes, err := queries.JobNodeGetManyByJobID(ctx, in.JobID)
	if err != nil {
		return db.Run{}, 0, fmt.Errorf("load job nodes: %w", err)
	}
	if len(nodes) == 0 {
		return db.Run{}, 0, errors.New("job has no nodes")
	}
	queuedAt := strings.TrimSpace(in.QueuedAt)
	if queuedAt == "" {
		queuedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	run, err := queries.RunCreate(ctx, db.RunCreateParams{
		RunKey:       fmt.Sprintf("run_%d", time.Now().UTC().UnixNano()),
		JobID:        in.JobID,
		Status:       "queued",
		TriggeredBy:  nonEmpty(in.TriggeredBy, "manual"),
		ParamsJson:   nonEmpty(in.ParamsJSON, "{}"),
		QueuedAt:     queuedAt,
		StartedAt:    "",
		CompletedAt:  "",
		ParentRunID:  in.ParentRunID,
		RerunStepKey: in.RerunStepKey,
		ErrorMessage: in.ErrorMessage,
	})
	if err != nil {
		return db.Run{}, 0, err
	}
	for _, node := range nodes {
		if _, err := queries.RunStepCreate(ctx, db.RunStepCreateParams{
			RunID:        run.ID,
			JobNodeID:    node.ID,
			StepKey:      node.StepKey,
			Status:       "pending",
			Attempt:      1,
			StartedAt:    "",
			CompletedAt:  "",
			DurationMs:   0,
			OutputJson:   "{}",
			ErrorMessage: "",
			LogExcerpt:   "",
		}); err != nil {
			return db.Run{}, 0, err
		}
	}
	eventPayload := map[string]any{
		"triggered_by":   run.TriggeredBy,
		"parent_run_id":  run.ParentRunID,
		"rerun_step_key": run.RerunStepKey,
	}
	for key, value := range in.EventPayload {
		eventPayload[key] = value
	}
	payloadBytes, _ := json.Marshal(eventPayload)
	if _, err := queries.RunEventCreate(ctx, db.RunEventCreateParams{
		RunID:         run.ID,
		StepKey:       "",
		EventType:     "run_queued",
		Level:         "info",
		Message:       "run queued",
		EventDataJson: string(payloadBytes),
	}); err != nil {
		return db.Run{}, 0, err
	}
	return run, len(nodes), nil
}

func nonEmpty(value string, fallback string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return fallback
	}
	return trimmed
}
