package backfills

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
	"github.com/swetjen/daggo/deps"
	"github.com/swetjen/virtuous/rpc"
)

const (
	backfillPartitionStatusTargeted         = "targeted"
	backfillPartitionStatusRequested        = "requested"
	backfillPartitionStatusMaterialized     = "materialized"
	backfillPartitionStatusFailedDownstream = "failed_downstream"

	maxPartitionKeysPerDefinition = 100000
	maxPartitionSelectionCount    = 100000
	maxBackfillRunsPerLaunch      = 5000
	maxPartitionsPerRun           = 1000
)

type Handlers struct {
	app *deps.Deps
}

func New(app *deps.Deps) *Handlers {
	return &Handlers{app: app}
}

type BackfillsGetManyRequest struct {
	JobKey string `json:"job_key"`
	Limit  int64  `json:"limit"`
	Offset int64  `json:"offset"`
}

type BackfillByKeyRequest struct {
	BackfillKey     string `json:"backfill_key"`
	PartitionLimit  int64  `json:"partition_limit"`
	PartitionOffset int64  `json:"partition_offset"`
}

type BackfillLaunchRequest struct {
	JobKey              string   `json:"job_key"`
	SelectionMode       string   `json:"selection_mode"`
	PartitionKey        string   `json:"partition_key"`
	RangeStartPartition string   `json:"range_start_partition"`
	RangeEndPartition   string   `json:"range_end_partition"`
	PartitionKeys       []string `json:"partition_keys"`
	PolicyMode          string   `json:"policy_mode"`
	MaxPartitionsPerRun int64    `json:"max_partitions_per_run"`
	TriggeredBy         string   `json:"triggered_by"`
}

type BackfillSummary struct {
	ID                      int64  `json:"id"`
	BackfillKey             string `json:"backfill_key"`
	JobID                   int64  `json:"job_id"`
	JobKey                  string `json:"job_key"`
	Status                  string `json:"status"`
	SelectionMode           string `json:"selection_mode"`
	TriggeredBy             string `json:"triggered_by"`
	PolicyMode              string `json:"policy_mode"`
	MaxPartitionsPerRun     int64  `json:"max_partitions_per_run"`
	RequestedPartitionCount int64  `json:"requested_partition_count"`
	RequestedRunCount       int64  `json:"requested_run_count"`
	CompletedPartitionCount int64  `json:"completed_partition_count"`
	FailedPartitionCount    int64  `json:"failed_partition_count"`
	TargetedCount           int64  `json:"targeted_count"`
	QueuedCount             int64  `json:"queued_count"`
	MaterializedCount       int64  `json:"materialized_count"`
	FailedDownstreamCount   int64  `json:"failed_downstream_count"`
	TotalTrackedPartitions  int64  `json:"total_tracked_partitions"`
	IsComplete              bool   `json:"is_complete"`
	ErrorMessage            string `json:"error_message"`
	StartedAt               string `json:"started_at"`
	CompletedAt             string `json:"completed_at"`
	CreatedAt               string `json:"created_at"`
	UpdatedAt               string `json:"updated_at"`
}

type BackfillPartition struct {
	PartitionKey string `json:"partition_key"`
	Status       string `json:"status"`
	RunID        int64  `json:"run_id"`
	ErrorMessage string `json:"error_message"`
	CreatedAt    string `json:"created_at"`
	UpdatedAt    string `json:"updated_at"`
}

type BackfillsGetManyResponse struct {
	Data             []BackfillSummary `json:"data"`
	Total            int64             `json:"total"`
	JobHasPartitions bool              `json:"job_has_partitions"`
	Error            string            `json:"error,omitempty"`
}

type BackfillByKeyResponse struct {
	Backfill        BackfillSummary     `json:"backfill"`
	Partitions      []BackfillPartition `json:"partitions"`
	PartitionsTotal int64               `json:"partitions_total"`
	Error           string              `json:"error,omitempty"`
}

type BackfillLaunchResponse struct {
	Backfill BackfillSummary `json:"backfill"`
	RunIDs   []int64         `json:"run_ids"`
	Error    string          `json:"error,omitempty"`
}

func (h *Handlers) BackfillsGetMany(ctx context.Context, req BackfillsGetManyRequest) (BackfillsGetManyResponse, int) {
	if h.app == nil || h.app.DB == nil {
		return BackfillsGetManyResponse{Error: "db is unavailable"}, rpc.StatusError
	}
	limit, offset := normalizePagination(req.Limit, req.Offset)

	var (
		rows             []db.Backfill
		total            int64
		jobHasPartitions bool
		err              error
	)
	jobKey := strings.TrimSpace(req.JobKey)
	if jobKey != "" {
		job, lookupErr := h.app.DB.JobGetByKey(ctx, jobKey)
		if lookupErr != nil {
			if errors.Is(lookupErr, sql.ErrNoRows) {
				return BackfillsGetManyResponse{Data: []BackfillSummary{}, Total: 0, JobHasPartitions: false}, rpc.StatusOK
			}
			return BackfillsGetManyResponse{Error: "failed to load job"}, rpc.StatusError
		}
		_, foundPartitionDefinition, partitionErr := h.resolveJobPartitionDefinition(ctx, job.ID)
		if partitionErr != nil {
			return BackfillsGetManyResponse{Error: "failed to load partition definition"}, rpc.StatusError
		}
		if foundPartitionDefinition {
			jobHasPartitions = true
		}
		rows, err = h.app.DB.BackfillGetManyByJobID(ctx, db.BackfillGetManyByJobIDParams{
			JobID:  job.ID,
			Limit:  limit,
			Offset: offset,
		})
		if err != nil {
			return BackfillsGetManyResponse{Error: "failed to load backfills"}, rpc.StatusError
		}
		total, err = h.app.DB.BackfillCountByJobID(ctx, job.ID)
		if err != nil {
			return BackfillsGetManyResponse{Error: "failed to count backfills"}, rpc.StatusError
		}
	} else {
		rows, err = h.app.DB.BackfillGetMany(ctx, db.BackfillGetManyParams{
			Limit:  limit,
			Offset: offset,
		})
		if err != nil {
			return BackfillsGetManyResponse{Error: "failed to load backfills"}, rpc.StatusError
		}
		total, err = h.app.DB.BackfillCount(ctx)
		if err != nil {
			return BackfillsGetManyResponse{Error: "failed to count backfills"}, rpc.StatusError
		}
	}

	data := make([]BackfillSummary, 0, len(rows))
	for _, row := range rows {
		summary, summaryErr := h.toBackfillSummary(ctx, row)
		if summaryErr != nil {
			return BackfillsGetManyResponse{Error: "failed to load backfill state"}, rpc.StatusError
		}
		data = append(data, summary)
	}
	return BackfillsGetManyResponse{Data: data, Total: total, JobHasPartitions: jobHasPartitions}, rpc.StatusOK
}

func (h *Handlers) BackfillByKey(ctx context.Context, req BackfillByKeyRequest) (BackfillByKeyResponse, int) {
	if h.app == nil || h.app.DB == nil {
		return BackfillByKeyResponse{Error: "db is unavailable"}, rpc.StatusError
	}
	backfillKey := strings.TrimSpace(req.BackfillKey)
	if backfillKey == "" {
		return BackfillByKeyResponse{Error: "backfill_key is required"}, rpc.StatusInvalid
	}

	row, err := h.app.DB.BackfillGetByKey(ctx, backfillKey)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return BackfillByKeyResponse{Error: "backfill not found"}, rpc.StatusInvalid
		}
		return BackfillByKeyResponse{Error: "failed to load backfill"}, rpc.StatusError
	}
	summary, err := h.toBackfillSummary(ctx, row)
	if err != nil {
		return BackfillByKeyResponse{Error: "failed to load backfill state"}, rpc.StatusError
	}

	partitionLimit, partitionOffset := normalizePagination(req.PartitionLimit, req.PartitionOffset)
	partitionRows, err := h.app.DB.BackfillPartitionGetManyByBackfillID(ctx, db.BackfillPartitionGetManyByBackfillIDParams{
		BackfillID: row.ID,
		Limit:      partitionLimit,
		Offset:     partitionOffset,
	})
	if err != nil {
		return BackfillByKeyResponse{Error: "failed to load backfill partitions"}, rpc.StatusError
	}
	partitionTotal, err := h.app.DB.BackfillPartitionCountByBackfillID(ctx, row.ID)
	if err != nil {
		return BackfillByKeyResponse{Error: "failed to count backfill partitions"}, rpc.StatusError
	}

	partitions := make([]BackfillPartition, 0, len(partitionRows))
	for _, partition := range partitionRows {
		partitions = append(partitions, BackfillPartition{
			PartitionKey: partition.PartitionKey,
			Status:       partition.Status,
			RunID:        partition.RunID,
			ErrorMessage: partition.ErrorMessage,
			CreatedAt:    partition.CreatedAt,
			UpdatedAt:    partition.UpdatedAt,
		})
	}
	return BackfillByKeyResponse{
		Backfill:        summary,
		Partitions:      partitions,
		PartitionsTotal: partitionTotal,
	}, rpc.StatusOK
}

func (h *Handlers) BackfillLaunch(ctx context.Context, req BackfillLaunchRequest) (BackfillLaunchResponse, int) {
	if h.app == nil || h.app.DB == nil || h.app.Pool == nil {
		return BackfillLaunchResponse{Error: "runtime dependencies are unavailable"}, rpc.StatusError
	}
	jobKey := strings.TrimSpace(req.JobKey)
	if jobKey == "" {
		return BackfillLaunchResponse{Error: "job_key is required"}, rpc.StatusInvalid
	}
	job, err := h.app.DB.JobGetByKey(ctx, jobKey)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return BackfillLaunchResponse{Error: "job not found"}, rpc.StatusInvalid
		}
		return BackfillLaunchResponse{Error: "failed to load job"}, rpc.StatusError
	}

	partitionDefinition, foundPartitionDefinition, err := h.resolveJobPartitionDefinition(ctx, job.ID)
	if err != nil {
		return BackfillLaunchResponse{Error: "failed to load partition definition"}, rpc.StatusError
	}
	if !foundPartitionDefinition {
		return BackfillLaunchResponse{Error: "job has no partition definition"}, rpc.StatusInvalid
	}
	totalPartitionKeys, err := h.app.DB.PartitionKeyCountByDefinitionID(ctx, partitionDefinition.ID)
	if err != nil {
		return BackfillLaunchResponse{Error: "failed to count partition keys"}, rpc.StatusError
	}
	if totalPartitionKeys <= 0 {
		return BackfillLaunchResponse{Error: "partition definition has no partition keys"}, rpc.StatusInvalid
	}
	if totalPartitionKeys > maxPartitionKeysPerDefinition {
		return BackfillLaunchResponse{Error: fmt.Sprintf("partition definition exceeds max allowed keys (%d)", maxPartitionKeysPerDefinition)}, rpc.StatusInvalid
	}

	partitionRows, err := h.app.DB.PartitionKeyGetManyByDefinitionID(ctx, db.PartitionKeyGetManyByDefinitionIDParams{
		PartitionDefinitionID: partitionDefinition.ID,
		Limit:                 totalPartitionKeys,
		Offset:                0,
	})
	if err != nil {
		return BackfillLaunchResponse{Error: "failed to load partition keys"}, rpc.StatusError
	}
	allPartitionKeys := make([]string, 0, len(partitionRows))
	for _, row := range partitionRows {
		allPartitionKeys = append(allPartitionKeys, row.PartitionKey)
	}

	selection, err := normalizeLaunchSelection(allPartitionKeys, req)
	if err != nil {
		return BackfillLaunchResponse{Error: err.Error()}, rpc.StatusInvalid
	}
	if len(selection.Keys) > maxPartitionSelectionCount {
		return BackfillLaunchResponse{Error: fmt.Sprintf("selection exceeds max allowed partitions (%d)", maxPartitionSelectionCount)}, rpc.StatusInvalid
	}

	policy, err := normalizeLaunchPolicy(req)
	if err != nil {
		return BackfillLaunchResponse{Error: err.Error()}, rpc.StatusInvalid
	}
	backfillPlan, err := dag.BuildBackfillPlan(selection, policy)
	if err != nil {
		return BackfillLaunchResponse{Error: err.Error()}, rpc.StatusInvalid
	}
	if len(backfillPlan.RunTargets) > maxBackfillRunsPerLaunch {
		return BackfillLaunchResponse{Error: fmt.Sprintf("backfill plan exceeds max run count (%d)", maxBackfillRunsPerLaunch)}, rpc.StatusInvalid
	}
	if len(backfillPlan.RunTargets) == 0 {
		return BackfillLaunchResponse{Error: "backfill plan produced zero runs"}, rpc.StatusInvalid
	}

	slog.Info("daggo: backfill launch request normalized",
		"job_key", jobKey,
		"selection_mode", selection.Mode,
		"selection_partition_count", len(selection.Keys),
		"policy_mode", policy.Mode,
		"max_partitions_per_run", policy.MaxPartitionsPerRun,
		"planned_runs", len(backfillPlan.RunTargets),
	)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	backfillKey := fmt.Sprintf("bf_%d", time.Now().UTC().UnixNano())
	selectionJSON, _ := json.Marshal(map[string]any{
		"mode":   selection.Mode,
		"keys":   selection.Keys,
		"ranges": selection.Ranges,
	})

	tx, err := h.app.Pool.BeginTx(ctx, nil)
	if err != nil {
		return BackfillLaunchResponse{Error: "failed to open transaction"}, rpc.StatusError
	}
	qtx := db.WithTx(h.app.DB, tx)

	backfillRow, err := qtx.BackfillCreate(ctx, db.BackfillCreateParams{
		BackfillKey:             backfillKey,
		JobID:                   job.ID,
		PartitionDefinitionID:   partitionDefinition.ID,
		Status:                  "running",
		SelectionMode:           string(selection.Mode),
		SelectionJson:           string(selectionJSON),
		TriggeredBy:             nonEmpty(strings.TrimSpace(req.TriggeredBy), "backfill"),
		PolicyMode:              string(policy.Mode),
		MaxPartitionsPerRun:     int64(policy.MaxPartitionsPerRun),
		RequestedPartitionCount: int64(len(selection.Keys)),
		RequestedRunCount:       int64(len(backfillPlan.RunTargets)),
		CompletedPartitionCount: 0,
		FailedPartitionCount:    0,
		ErrorMessage:            "",
		StartedAt:               now,
		CompletedAt:             "",
	})
	if err != nil {
		_ = tx.Rollback()
		return BackfillLaunchResponse{Error: "failed to create backfill"}, rpc.StatusError
	}

	for _, partitionKey := range selection.Keys {
		if _, err := qtx.BackfillPartitionUpsert(ctx, db.BackfillPartitionUpsertParams{
			BackfillID:   backfillRow.ID,
			PartitionKey: partitionKey,
			Status:       backfillPartitionStatusTargeted,
			RunID:        0,
			ErrorMessage: "",
		}); err != nil {
			_ = tx.Rollback()
			return BackfillLaunchResponse{Error: "failed to create backfill partition subset"}, rpc.StatusError
		}
	}

	runIDs := make([]int64, 0, len(backfillPlan.RunTargets))
	for _, target := range backfillPlan.RunTargets {
		paramsJSON, err := json.Marshal(map[string]any{
			"backfill_key":   backfillKey,
			"partition_keys": target.Selection.Keys,
		})
		if err != nil {
			_ = tx.Rollback()
			return BackfillLaunchResponse{Error: "failed to prepare run params"}, rpc.StatusError
		}

		runRow, err := h.createRunWithStepsTx(ctx, qtx, createRunInput{
			JobID:       job.ID,
			TriggeredBy: nonEmpty(strings.TrimSpace(req.TriggeredBy), "backfill"),
			ParamsJSON:  string(paramsJSON),
		})
		if err != nil {
			_ = tx.Rollback()
			return BackfillLaunchResponse{Error: "failed to create backfill run"}, rpc.StatusError
		}
		runIDs = append(runIDs, runRow.ID)

		renderedTags, err := dag.RenderRunTargetTags(dag.RunTarget{
			Selection:   target.Selection,
			BackfillKey: backfillKey,
		}, dag.PartitionTagProjectionDaggo)
		if err != nil {
			_ = tx.Rollback()
			return BackfillLaunchResponse{Error: "failed to render run tags"}, rpc.StatusError
		}
		tagsJSON, _ := json.Marshal(renderedTags)
		subsetJSON, _ := json.Marshal(target.Selection.Keys)
		rangeStart := ""
		rangeEnd := ""
		if len(target.Selection.Ranges) > 0 {
			rangeStart = target.Selection.Ranges[0].StartKey
			rangeEnd = target.Selection.Ranges[0].EndKey
		}
		partitionKey := ""
		if target.Selection.Mode == dag.PartitionSelectionModeSingle && len(target.Selection.Keys) == 1 {
			partitionKey = target.Selection.Keys[0]
		}

		if _, err := qtx.RunPartitionTargetUpsert(ctx, db.RunPartitionTargetUpsertParams{
			RunID:                 runRow.ID,
			PartitionDefinitionID: partitionDefinition.ID,
			SelectionMode:         string(target.Selection.Mode),
			PartitionKey:          partitionKey,
			RangeStartKey:         rangeStart,
			RangeEndKey:           rangeEnd,
			PartitionSubsetJson:   string(subsetJSON),
			TagsJson:              string(tagsJSON),
			BackfillKey:           backfillKey,
		}); err != nil {
			_ = tx.Rollback()
			return BackfillLaunchResponse{Error: "failed to persist run partition target"}, rpc.StatusError
		}
		for tagKey, tagValue := range renderedTags {
			if _, err := qtx.RunSystemTagUpsert(ctx, db.RunSystemTagUpsertParams{
				RunID:    runRow.ID,
				TagKey:   tagKey,
				TagValue: tagValue,
			}); err != nil {
				_ = tx.Rollback()
				return BackfillLaunchResponse{Error: "failed to persist run system tags"}, rpc.StatusError
			}
		}

		for _, partitionKey := range target.Selection.Keys {
			if _, err := qtx.BackfillPartitionUpdateStatusByBackfillIDAndPartitionKey(ctx, db.BackfillPartitionUpdateStatusByBackfillIDAndPartitionKeyParams{
				Status:       backfillPartitionStatusRequested,
				RunID:        runRow.ID,
				ErrorMessage: "",
				BackfillID:   backfillRow.ID,
				PartitionKey: partitionKey,
			}); err != nil {
				_ = tx.Rollback()
				return BackfillLaunchResponse{Error: "failed to mark backfill partition as requested"}, rpc.StatusError
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return BackfillLaunchResponse{Error: "failed to commit backfill launch"}, rpc.StatusError
	}
	if h.app.Executor != nil {
		for _, runID := range runIDs {
			h.app.Executor.EnqueueRun(runID)
		}
	}

	createdBackfill, err := h.app.DB.BackfillGetByKey(ctx, backfillKey)
	if err != nil {
		return BackfillLaunchResponse{Error: "failed to load launched backfill"}, rpc.StatusError
	}
	summary, err := h.toBackfillSummary(ctx, createdBackfill)
	if err != nil {
		return BackfillLaunchResponse{Error: "failed to load launched backfill summary"}, rpc.StatusError
	}

	slog.Info("daggo: backfill launched",
		"backfill_key", backfillKey,
		"job_key", jobKey,
		"runs_created", len(runIDs),
		"partitions_requested", len(selection.Keys),
	)
	return BackfillLaunchResponse{Backfill: summary, RunIDs: runIDs}, rpc.StatusOK
}

type createRunInput struct {
	JobID       int64
	TriggeredBy string
	ParamsJSON  string
}

func (h *Handlers) createRunWithStepsTx(ctx context.Context, store db.Store, in createRunInput) (db.Run, error) {
	nodes, err := store.JobNodeGetManyByJobID(ctx, in.JobID)
	if err != nil {
		return db.Run{}, err
	}
	if len(nodes) == 0 {
		return db.Run{}, errors.New("job has no nodes")
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	runRow, err := store.RunCreate(ctx, db.RunCreateParams{
		RunKey:       fmt.Sprintf("run_%d", time.Now().UTC().UnixNano()),
		JobID:        in.JobID,
		Status:       "queued",
		TriggeredBy:  nonEmpty(in.TriggeredBy, "manual"),
		ParamsJson:   nonEmpty(in.ParamsJSON, "{}"),
		QueuedAt:     now,
		StartedAt:    "",
		CompletedAt:  "",
		ParentRunID:  0,
		RerunStepKey: "",
		ErrorMessage: "",
	})
	if err != nil {
		return db.Run{}, err
	}
	for _, node := range nodes {
		if _, err := store.RunStepCreate(ctx, db.RunStepCreateParams{
			RunID:        runRow.ID,
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
			return db.Run{}, err
		}
	}
	eventPayload, _ := json.Marshal(map[string]any{
		"triggered_by": in.TriggeredBy,
		"backfill":     true,
	})
	if _, err := store.RunEventCreate(ctx, db.RunEventCreateParams{
		RunID:         runRow.ID,
		StepKey:       "",
		EventType:     "run_queued",
		Level:         "info",
		Message:       "run queued",
		EventDataJson: string(eventPayload),
	}); err != nil {
		return db.Run{}, err
	}
	return runRow, nil
}

func (h *Handlers) toBackfillSummary(ctx context.Context, row db.Backfill) (BackfillSummary, error) {
	job, err := h.app.DB.JobGetByID(ctx, row.JobID)
	if err != nil {
		return BackfillSummary{}, err
	}

	totalTrackedPartitions, err := h.app.DB.BackfillPartitionCountByBackfillID(ctx, row.ID)
	if err != nil {
		return BackfillSummary{}, err
	}
	targetedCount, err := h.statusCount(ctx, row.ID, backfillPartitionStatusTargeted)
	if err != nil {
		return BackfillSummary{}, err
	}
	queuedCount, err := h.statusCount(ctx, row.ID, backfillPartitionStatusRequested)
	if err != nil {
		return BackfillSummary{}, err
	}
	materializedCount, err := h.statusCount(ctx, row.ID, backfillPartitionStatusMaterialized)
	if err != nil {
		return BackfillSummary{}, err
	}
	failedDownstreamCount, err := h.statusCount(ctx, row.ID, backfillPartitionStatusFailedDownstream)
	if err != nil {
		return BackfillSummary{}, err
	}

	accounted := materializedCount + failedDownstreamCount
	isTerminal := isTerminalBackfillStatus(row.Status)
	isComplete := totalTrackedPartitions > 0 && isTerminal && accounted >= totalTrackedPartitions

	return BackfillSummary{
		ID:                      row.ID,
		BackfillKey:             row.BackfillKey,
		JobID:                   row.JobID,
		JobKey:                  job.JobKey,
		Status:                  row.Status,
		SelectionMode:           row.SelectionMode,
		TriggeredBy:             row.TriggeredBy,
		PolicyMode:              row.PolicyMode,
		MaxPartitionsPerRun:     row.MaxPartitionsPerRun,
		RequestedPartitionCount: row.RequestedPartitionCount,
		RequestedRunCount:       row.RequestedRunCount,
		CompletedPartitionCount: row.CompletedPartitionCount,
		FailedPartitionCount:    row.FailedPartitionCount,
		TargetedCount:           targetedCount,
		QueuedCount:             queuedCount,
		MaterializedCount:       materializedCount,
		FailedDownstreamCount:   failedDownstreamCount,
		TotalTrackedPartitions:  totalTrackedPartitions,
		IsComplete:              isComplete,
		ErrorMessage:            row.ErrorMessage,
		StartedAt:               row.StartedAt,
		CompletedAt:             row.CompletedAt,
		CreatedAt:               row.CreatedAt,
		UpdatedAt:               row.UpdatedAt,
	}, nil
}

func (h *Handlers) statusCount(ctx context.Context, backfillID int64, status string) (int64, error) {
	return h.app.DB.BackfillPartitionCountByBackfillIDAndStatus(ctx, db.BackfillPartitionCountByBackfillIDAndStatusParams{
		BackfillID: backfillID,
		Status:     status,
	})
}

func isTerminalBackfillStatus(status string) bool {
	switch strings.TrimSpace(strings.ToLower(status)) {
	case "completed", "failed", "canceled":
		return true
	default:
		return false
	}
}

func (h *Handlers) resolveJobPartitionDefinition(ctx context.Context, jobID int64) (db.PartitionDefinition, bool, error) {
	definitions, err := h.app.DB.PartitionDefinitionGetManyByJobID(ctx, jobID)
	if err != nil {
		return db.PartitionDefinition{}, false, err
	}
	opDefinitions := make([]db.PartitionDefinition, 0)
	for _, definition := range definitions {
		if definition.TargetKind != "op" {
			continue
		}
		if strings.TrimSpace(definition.TargetKey) == "" {
			continue
		}
		if definition.Enabled == 0 {
			continue
		}
		opDefinitions = append(opDefinitions, definition)
	}
	if len(opDefinitions) > 0 {
		base := opDefinitions[0]
		for _, definition := range opDefinitions[1:] {
			if definition.DefinitionKind != base.DefinitionKind || definition.DefinitionJson != base.DefinitionJson {
				return db.PartitionDefinition{}, false, fmt.Errorf("job has multiple op partition definitions with different domains")
			}
		}
		return base, true, nil
	}

	legacy, err := h.app.DB.PartitionDefinitionGetByJobIDAndTarget(ctx, db.PartitionDefinitionGetByJobIDAndTargetParams{
		JobID:      jobID,
		TargetKind: "job",
		TargetKey:  "",
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return db.PartitionDefinition{}, false, nil
		}
		return db.PartitionDefinition{}, false, err
	}
	if legacy.Enabled == 0 {
		return db.PartitionDefinition{}, false, nil
	}
	return legacy, true, nil
}

func normalizePagination(limit, offset int64) (int64, int64) {
	if limit <= 0 {
		limit = 25
	}
	if limit > 200 {
		limit = 200
	}
	if offset < 0 {
		offset = 0
	}
	return limit, offset
}

func normalizeLaunchSelection(allPartitionKeys []string, req BackfillLaunchRequest) (dag.NormalizedPartitionSelection, error) {
	mode := strings.TrimSpace(strings.ToLower(req.SelectionMode))
	if mode == "" {
		mode = "subset"
	}
	var selection dag.PartitionSelection
	switch mode {
	case "all":
		selection = dag.SelectPartitionSubset(allPartitionKeys...)
	case "single":
		selection = dag.SelectPartitionKey(req.PartitionKey)
	case "range":
		selection = dag.SelectPartitionRange(req.RangeStartPartition, req.RangeEndPartition)
	case "subset":
		selection = dag.SelectPartitionSubset(req.PartitionKeys...)
	default:
		return dag.NormalizedPartitionSelection{}, fmt.Errorf("unsupported selection_mode %q", req.SelectionMode)
	}
	return dag.NormalizePartitionSelection(allPartitionKeys, selection)
}

func normalizeLaunchPolicy(req BackfillLaunchRequest) (dag.BackfillPolicy, error) {
	mode := strings.TrimSpace(strings.ToLower(req.PolicyMode))
	if mode == "" {
		mode = string(dag.BackfillPolicyMultiRun)
	}
	switch mode {
	case string(dag.BackfillPolicySingleRun):
		return dag.BackfillPolicy{Mode: dag.BackfillPolicySingleRun}, nil
	case string(dag.BackfillPolicyMultiRun):
		maxPerRun := int(req.MaxPartitionsPerRun)
		if maxPerRun <= 0 {
			maxPerRun = 1
		}
		if maxPerRun > maxPartitionsPerRun {
			return dag.BackfillPolicy{}, fmt.Errorf("max_partitions_per_run exceeds allowed limit (%d)", maxPartitionsPerRun)
		}
		return dag.BackfillPolicy{
			Mode:                dag.BackfillPolicyMultiRun,
			MaxPartitionsPerRun: maxPerRun,
		}, nil
	default:
		return dag.BackfillPolicy{}, fmt.Errorf("unsupported policy_mode %q", req.PolicyMode)
	}
}

func nonEmpty(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
