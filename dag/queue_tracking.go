package dag

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/swetjen/daggo/db"
)

func RecordQueueStepMetadata(ctx context.Context, queries db.Store, queueMeta *RunQueueMeta, jobID int64, runID int64, stepKey string, metadata map[string]any) error {
	if queries == nil || queueMeta == nil || queueMeta.QueueItemID <= 0 || jobID <= 0 || runID <= 0 {
		return nil
	}
	if len(metadata) == 0 {
		return nil
	}
	payload, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	_, err = queries.QueueItemStepMetadataUpsert(ctx, db.QueueItemStepMetadataUpsertParams{
		QueueItemID: queueMeta.QueueItemID,
		JobID:       jobID,
		RunID:       runID,
		StepKey:     stepKey,
		MetadataJson: string(payload),
	})
	return err
}

func SyncLinkedQueueItemStatus(ctx context.Context, queries db.Store, runID int64) error {
	if queries == nil || runID <= 0 {
		return nil
	}
	link, err := queries.QueueItemRunGetByRunID(ctx, runID)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
		return err
	}
	rows, err := queries.QueueItemRunGetManyByQueueItemIDJoinedRuns(ctx, link.QueueItemID)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	status, startedAt, completedAt, errorMessage := aggregateQueueItemLifecycle(rows)
	_, err = queries.QueueItemUpdateLifecycleByID(ctx, db.QueueItemUpdateLifecycleByIDParams{
		Status:       status,
		StartedAt:    startedAt,
		CompletedAt:  completedAt,
		ErrorMessage: errorMessage,
		ID:           link.QueueItemID,
	})
	return err
}

func aggregateQueueItemLifecycle(rows []db.QueueItemRunGetManyByQueueItemIDJoinedRunsRow) (string, string, string, string) {
	allQueued := true
	anyNonTerminal := false
	allSuccess := true
	var (
		firstStarted   time.Time
		latestComplete time.Time
		errorMessage   string
	)
	for _, row := range rows {
		status := normalizeExecutionStatus(row.RunStatus)
		if status != "queued" {
			allQueued = false
		}
		if status == "queued" || status == "running" {
			anyNonTerminal = true
		}
		if status != "success" {
			allSuccess = false
		}
		if row.StartedAt != "" {
			started := parseQueueStoredTime(row.StartedAt)
			if !started.IsZero() && (firstStarted.IsZero() || started.Before(firstStarted)) {
				firstStarted = started
			}
		}
		if row.CompletedAt != "" {
			completed := parseQueueStoredTime(row.CompletedAt)
			if !completed.IsZero() && completed.After(latestComplete) {
				latestComplete = completed
			}
		}
		if errorMessage == "" && row.RunErrorMessage != "" {
			errorMessage = row.RunErrorMessage
		}
	}
	queueStatus := "queued"
	switch {
	case allQueued:
		queueStatus = "queued"
	case anyNonTerminal:
		queueStatus = "running"
	case allSuccess:
		queueStatus = "complete"
	default:
		queueStatus = "failed"
	}
	startedAt := ""
	if !firstStarted.IsZero() {
		startedAt = firstStarted.UTC().Format(time.RFC3339Nano)
	}
	completedAt := ""
	if (queueStatus == "complete" || queueStatus == "failed") && !latestComplete.IsZero() {
		completedAt = latestComplete.UTC().Format(time.RFC3339Nano)
	}
	if queueStatus == "complete" {
		errorMessage = ""
	}
	return queueStatus, startedAt, completedAt, errorMessage
}

func parseQueueStoredTime(value string) time.Time {
	if value == "" {
		return time.Time{}
	}
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return parsed.UTC()
	}
	if parsed, err := time.Parse(time.RFC3339, value); err == nil {
		return parsed.UTC()
	}
	if parsed, err := time.Parse("2006-01-02 15:04:05", value); err == nil {
		return parsed.UTC()
	}
	return time.Time{}
}
