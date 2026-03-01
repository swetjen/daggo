package runs

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/swetjen/daggo/db"
	"github.com/swetjen/daggo/deps"
	"github.com/swetjen/virtuous/rpc"
)

type Handlers struct {
	app *deps.Deps
}

func New(app *deps.Deps) *Handlers {
	return &Handlers{app: app}
}

type RunCreateRequest struct {
	JobKey      string `json:"job_key"`
	TriggeredBy string `json:"triggered_by"`
	Note        string `json:"note"`
}

type RunRerunStepCreateRequest struct {
	SourceRunID int64  `json:"source_run_id"`
	StepKey     string `json:"step_key"`
	TriggeredBy string `json:"triggered_by"`
}

type RunsGetManyRequest struct {
	JobKey string `json:"job_key"`
	Limit  int64  `json:"limit"`
	Offset int64  `json:"offset"`
}

type RunByIDRequest struct {
	ID int64 `json:"id"`
}

type RunTerminateRequest struct {
	ID int64 `json:"id"`
}

type RunEventsGetManyRequest struct {
	RunID  int64 `json:"run_id"`
	Limit  int64 `json:"limit"`
	Offset int64 `json:"offset"`
	Tail   bool  `json:"tail"`
}

type RunStep struct {
	StepKey      string `json:"step_key"`
	Status       string `json:"status"`
	Attempt      int64  `json:"attempt"`
	StartedAt    string `json:"started_at"`
	CompletedAt  string `json:"completed_at"`
	DurationMs   int64  `json:"duration_ms"`
	OutputJSON   string `json:"output_json"`
	ErrorMessage string `json:"error_message"`
	LogExcerpt   string `json:"log_excerpt"`
}

type RunEvent struct {
	ID            int64  `json:"id"`
	StepKey       string `json:"step_key"`
	EventType     string `json:"event_type"`
	Level         string `json:"level"`
	Message       string `json:"message"`
	EventDataJSON string `json:"event_data_json"`
	CreatedAt     string `json:"created_at"`
}

type RunSummary struct {
	ID           int64  `json:"id"`
	RunKey       string `json:"run_key"`
	JobID        int64  `json:"job_id"`
	JobKey       string `json:"job_key"`
	Status       string `json:"status"`
	TriggeredBy  string `json:"triggered_by"`
	QueuedAt     string `json:"queued_at"`
	StartedAt    string `json:"started_at"`
	CompletedAt  string `json:"completed_at"`
	ParentRunID  int64  `json:"parent_run_id"`
	RerunStepKey string `json:"rerun_step_key"`
	ErrorMessage string `json:"error_message"`
	PendingSteps int64  `json:"pending_steps"`
	RunningSteps int64  `json:"running_steps"`
	SuccessSteps int64  `json:"success_steps"`
	FailedSteps  int64  `json:"failed_steps"`
	SkippedSteps int64  `json:"skipped_steps"`
}

type Run struct {
	Summary RunSummary `json:"summary"`
	Steps   []RunStep  `json:"steps"`
}

type RunCreateResponse struct {
	Run   RunSummary `json:"run"`
	Error string     `json:"error,omitempty"`
}

type RunsGetManyResponse struct {
	Data  []RunSummary `json:"data"`
	Total int64        `json:"total"`
	Error string       `json:"error,omitempty"`
}

type RunByIDResponse struct {
	Run    Run        `json:"run"`
	Events []RunEvent `json:"events"`
	Error  string     `json:"error,omitempty"`
}

type RunTerminateResponse struct {
	Run   RunSummary `json:"run"`
	Error string     `json:"error,omitempty"`
}

type RunEventsGetManyResponse struct {
	Data  []RunEvent `json:"data"`
	Total int64      `json:"total"`
	Error string     `json:"error,omitempty"`
}

const (
	maxRunEventMessageBytes  = 2048
	maxRunEventDataJSONBytes = 8192
	deployDrainErrorMessage  = "deployment in progress; new runs are temporarily blocked"
)

func (h *Handlers) RunCreate(ctx context.Context, req RunCreateRequest) (RunCreateResponse, int) {
	if h.app != nil && h.app.DeployLock != nil && h.app.DeployLock.IsDraining() {
		return RunCreateResponse{Error: deployDrainErrorMessage}, rpc.StatusInvalid
	}
	jobKey := strings.TrimSpace(req.JobKey)
	if jobKey == "" {
		return RunCreateResponse{Error: "job_key is required"}, rpc.StatusInvalid
	}
	job, err := h.app.DB.JobGetByKey(ctx, jobKey)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return RunCreateResponse{Error: "job not found"}, rpc.StatusInvalid
		}
		return RunCreateResponse{Error: "failed to load job"}, rpc.StatusError
	}

	params := map[string]any{}
	if strings.TrimSpace(req.Note) != "" {
		params["note"] = strings.TrimSpace(req.Note)
	}
	paramsJSON, err := json.Marshal(params)
	if err != nil {
		return RunCreateResponse{Error: "failed to marshal params"}, rpc.StatusError
	}

	runSummary, err := h.createRunWithSteps(ctx, createRunInput{
		JobID:        job.ID,
		JobKey:       job.JobKey,
		TriggeredBy:  nonEmpty(req.TriggeredBy, "manual"),
		ParamsJSON:   string(paramsJSON),
		ParentRunID:  0,
		RerunStepKey: "",
	})
	if err != nil {
		return RunCreateResponse{Error: err.Error()}, rpc.StatusError
	}

	h.app.Executor.EnqueueRun(runSummary.ID)
	return RunCreateResponse{Run: runSummary}, rpc.StatusOK
}

func (h *Handlers) RunRerunStepCreate(ctx context.Context, req RunRerunStepCreateRequest) (RunCreateResponse, int) {
	if h.app != nil && h.app.DeployLock != nil && h.app.DeployLock.IsDraining() {
		return RunCreateResponse{Error: deployDrainErrorMessage}, rpc.StatusInvalid
	}
	if req.SourceRunID <= 0 {
		return RunCreateResponse{Error: "source_run_id is required"}, rpc.StatusInvalid
	}
	stepKey := strings.TrimSpace(req.StepKey)
	if stepKey == "" {
		return RunCreateResponse{Error: "step_key is required"}, rpc.StatusInvalid
	}

	sourceRun, err := h.app.DB.RunGetByID(ctx, req.SourceRunID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return RunCreateResponse{Error: "source run not found"}, rpc.StatusInvalid
		}
		return RunCreateResponse{Error: "failed to load source run"}, rpc.StatusError
	}

	if _, err := h.app.DB.RunStepGetByRunIDAndStepKey(ctx, db.RunStepGetByRunIDAndStepKeyParams{
		RunID:   req.SourceRunID,
		StepKey: stepKey,
	}); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return RunCreateResponse{Error: "step not found on source run"}, rpc.StatusInvalid
		}
		return RunCreateResponse{Error: "failed to validate source step"}, rpc.StatusError
	}

	runSummary, err := h.createRunWithSteps(ctx, createRunInput{
		JobID:        sourceRun.JobID,
		JobKey:       "",
		TriggeredBy:  nonEmpty(req.TriggeredBy, "rerun"),
		ParamsJSON:   sourceRun.ParamsJson,
		ParentRunID:  sourceRun.ID,
		RerunStepKey: stepKey,
	})
	if err != nil {
		return RunCreateResponse{Error: err.Error()}, rpc.StatusError
	}

	h.app.Executor.EnqueueRun(runSummary.ID)
	return RunCreateResponse{Run: runSummary}, rpc.StatusOK
}

func (h *Handlers) RunTerminate(ctx context.Context, req RunTerminateRequest) (RunTerminateResponse, int) {
	if req.ID <= 0 {
		return RunTerminateResponse{Error: "id is required"}, rpc.StatusInvalid
	}

	row, err := h.app.DB.RunGetByIDJoinedJobs(ctx, req.ID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return RunTerminateResponse{Error: "run not found"}, rpc.StatusInvalid
		}
		return RunTerminateResponse{Error: "failed to load run"}, rpc.StatusError
	}
	if isTerminalRunStatus(row.Status) {
		return RunTerminateResponse{Error: "run is already in a terminal state"}, rpc.StatusInvalid
	}

	if err := h.app.Executor.TerminateRun(req.ID); err != nil {
		return RunTerminateResponse{Error: err.Error()}, rpc.StatusInvalid
	}

	updated, err := h.app.DB.RunGetByIDJoinedJobs(ctx, req.ID)
	if err != nil {
		return RunTerminateResponse{Error: "failed to load updated run"}, rpc.StatusError
	}
	summary, err := h.decorateSummary(ctx, toRunSummaryByID(updated))
	if err != nil {
		return RunTerminateResponse{Error: "failed to load run step summary"}, rpc.StatusError
	}
	return RunTerminateResponse{Run: summary}, rpc.StatusOK
}

func (h *Handlers) RunsGetMany(ctx context.Context, req RunsGetManyRequest) (RunsGetManyResponse, int) {
	limit, offset := normalizePagination(req.Limit, req.Offset)

	if strings.TrimSpace(req.JobKey) != "" {
		job, err := h.app.DB.JobGetByKey(ctx, strings.TrimSpace(req.JobKey))
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return RunsGetManyResponse{Data: []RunSummary{}, Total: 0}, rpc.StatusOK
			}
			return RunsGetManyResponse{Error: "failed to load job"}, rpc.StatusError
		}
		rows, err := h.app.DB.RunGetManyByJobIDJoinedJobs(ctx, db.RunGetManyByJobIDJoinedJobsParams{
			JobID:  job.ID,
			Limit:  limit,
			Offset: offset,
		})
		if err != nil {
			return RunsGetManyResponse{Error: "failed to load runs"}, rpc.StatusError
		}
		total, err := h.app.DB.RunCountByJobID(ctx, job.ID)
		if err != nil {
			return RunsGetManyResponse{Error: "failed to count runs"}, rpc.StatusError
		}
		data, err := h.decorateSummaries(ctx, toRunSummariesByJob(rows))
		if err != nil {
			return RunsGetManyResponse{Error: "failed to load run steps"}, rpc.StatusError
		}
		return RunsGetManyResponse{Data: data, Total: total}, rpc.StatusOK
	}

	rows, err := h.app.DB.RunGetManyJoinedJobs(ctx, db.RunGetManyJoinedJobsParams{Limit: limit, Offset: offset})
	if err != nil {
		return RunsGetManyResponse{Error: "failed to load runs"}, rpc.StatusError
	}
	total, err := h.app.DB.RunCount(ctx)
	if err != nil {
		return RunsGetManyResponse{Error: "failed to count runs"}, rpc.StatusError
	}
	data, err := h.decorateSummaries(ctx, toRunSummaries(rows))
	if err != nil {
		return RunsGetManyResponse{Error: "failed to load run steps"}, rpc.StatusError
	}
	return RunsGetManyResponse{Data: data, Total: total}, rpc.StatusOK
}

func (h *Handlers) RunByID(ctx context.Context, req RunByIDRequest) (RunByIDResponse, int) {
	if req.ID <= 0 {
		return RunByIDResponse{Error: "id is required"}, rpc.StatusInvalid
	}
	row, err := h.app.DB.RunGetByIDJoinedJobs(ctx, req.ID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return RunByIDResponse{Error: "run not found"}, rpc.StatusInvalid
		}
		return RunByIDResponse{Error: "failed to load run"}, rpc.StatusError
	}
	summary, err := h.decorateSummary(ctx, toRunSummaryByID(row))
	if err != nil {
		return RunByIDResponse{Error: "failed to load run summary"}, rpc.StatusError
	}
	stepsRows, err := h.app.DB.RunStepGetManyByRunID(ctx, req.ID)
	if err != nil {
		return RunByIDResponse{Error: "failed to load run steps"}, rpc.StatusError
	}
	return RunByIDResponse{
		Run: Run{
			Summary: summary,
			Steps:   toRunSteps(stepsRows),
		},
		Events: []RunEvent{},
	}, rpc.StatusOK
}

func (h *Handlers) RunEventsGetMany(ctx context.Context, req RunEventsGetManyRequest) (RunEventsGetManyResponse, int) {
	if req.RunID <= 0 {
		return RunEventsGetManyResponse{Error: "run_id is required"}, rpc.StatusInvalid
	}
	limit, offset := normalizePagination(req.Limit, req.Offset)
	total, err := h.app.DB.RunEventCountByRunID(ctx, req.RunID)
	if err != nil {
		return RunEventsGetManyResponse{Error: "failed to count run events"}, rpc.StatusError
	}
	if req.Tail {
		if total > limit {
			offset = total - limit
		} else {
			offset = 0
		}
	}
	if total >= 0 && offset > total {
		offset = total
	}
	rows, err := h.app.DB.RunEventGetManyByRunID(ctx, db.RunEventGetManyByRunIDParams{
		RunID:  req.RunID,
		Limit:  limit,
		Offset: offset,
	})
	if err != nil {
		return RunEventsGetManyResponse{Error: "failed to load run events"}, rpc.StatusError
	}
	return RunEventsGetManyResponse{Data: toRunEvents(rows), Total: total}, rpc.StatusOK
}

type createRunInput struct {
	JobID        int64
	JobKey       string
	TriggeredBy  string
	ParamsJSON   string
	ParentRunID  int64
	RerunStepKey string
}

func (h *Handlers) createRunWithSteps(ctx context.Context, in createRunInput) (RunSummary, error) {
	nodes, err := h.app.DB.JobNodeGetManyByJobID(ctx, in.JobID)
	if err != nil {
		return RunSummary{}, fmt.Errorf("failed to load job nodes: %w", err)
	}
	if len(nodes) == 0 {
		return RunSummary{}, errors.New("job has no nodes")
	}

	tx, err := h.app.Pool.BeginTx(ctx, nil)
	if err != nil {
		return RunSummary{}, err
	}
	qtx := db.WithTx(h.app.DB, tx)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	runKey := fmt.Sprintf("run_%d", time.Now().UTC().UnixNano())

	run, err := qtx.RunCreate(ctx, db.RunCreateParams{
		RunKey:       runKey,
		JobID:        in.JobID,
		Status:       "queued",
		TriggeredBy:  nonEmpty(in.TriggeredBy, "manual"),
		ParamsJson:   nonEmpty(in.ParamsJSON, "{}"),
		QueuedAt:     now,
		StartedAt:    "",
		CompletedAt:  "",
		ParentRunID:  in.ParentRunID,
		RerunStepKey: in.RerunStepKey,
		ErrorMessage: "",
	})
	if err != nil {
		_ = tx.Rollback()
		return RunSummary{}, err
	}

	for _, node := range nodes {
		if _, err := qtx.RunStepCreate(ctx, db.RunStepCreateParams{
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
			_ = tx.Rollback()
			return RunSummary{}, err
		}
	}

	payloadBytes, _ := json.Marshal(map[string]any{
		"triggered_by":   run.TriggeredBy,
		"parent_run_id":  run.ParentRunID,
		"rerun_step_key": run.RerunStepKey,
	})
	if _, err := qtx.RunEventCreate(ctx, db.RunEventCreateParams{
		RunID:         run.ID,
		StepKey:       "",
		EventType:     "run_queued",
		Level:         "info",
		Message:       "run queued",
		EventDataJson: string(payloadBytes),
	}); err != nil {
		_ = tx.Rollback()
		return RunSummary{}, err
	}

	if err := tx.Commit(); err != nil {
		return RunSummary{}, err
	}

	jobKey := in.JobKey
	if strings.TrimSpace(jobKey) == "" {
		job, err := h.app.DB.JobGetByID(ctx, run.JobID)
		if err == nil {
			jobKey = job.JobKey
		}
	}
	return RunSummary{
		ID:           run.ID,
		RunKey:       run.RunKey,
		JobID:        run.JobID,
		JobKey:       jobKey,
		Status:       run.Status,
		TriggeredBy:  run.TriggeredBy,
		QueuedAt:     run.QueuedAt,
		StartedAt:    run.StartedAt,
		CompletedAt:  run.CompletedAt,
		ParentRunID:  run.ParentRunID,
		RerunStepKey: run.RerunStepKey,
		ErrorMessage: run.ErrorMessage,
		PendingSteps: int64(len(nodes)),
	}, nil
}

func (h *Handlers) decorateSummaries(ctx context.Context, rows []RunSummary) ([]RunSummary, error) {
	out := make([]RunSummary, 0, len(rows))
	for _, row := range rows {
		decorated, err := h.decorateSummary(ctx, row)
		if err != nil {
			return nil, err
		}
		out = append(out, decorated)
	}
	return out, nil
}

func (h *Handlers) decorateSummary(ctx context.Context, row RunSummary) (RunSummary, error) {
	steps, err := h.app.DB.RunStepGetManyByRunID(ctx, row.ID)
	if err != nil {
		return RunSummary{}, err
	}
	for _, step := range steps {
		switch step.Status {
		case "pending":
			row.PendingSteps++
		case "running":
			row.RunningSteps++
		case "success":
			row.SuccessSteps++
		case "failed":
			row.FailedSteps++
		case "skipped":
			row.SkippedSteps++
		}
	}
	return row, nil
}

func toRunSummaries(rows []db.RunGetManyJoinedJobsRow) []RunSummary {
	out := make([]RunSummary, 0, len(rows))
	for _, row := range rows {
		out = append(out, toRunSummary(row))
	}
	return out
}

func isTerminalRunStatus(status string) bool {
	normalized := strings.TrimSpace(strings.ToLower(status))
	switch normalized {
	case "success", "failed", "canceled", "cancelled":
		return true
	default:
		return false
	}
}

func toRunSummariesByJob(rows []db.RunGetManyByJobIDJoinedJobsRow) []RunSummary {
	out := make([]RunSummary, 0, len(rows))
	for _, row := range rows {
		out = append(out, RunSummary{
			ID:           row.ID,
			RunKey:       row.RunKey,
			JobID:        row.JobID,
			JobKey:       row.JobKey,
			Status:       row.Status,
			TriggeredBy:  row.TriggeredBy,
			QueuedAt:     row.QueuedAt,
			StartedAt:    row.StartedAt,
			CompletedAt:  row.CompletedAt,
			ParentRunID:  row.ParentRunID,
			RerunStepKey: row.RerunStepKey,
			ErrorMessage: row.ErrorMessage,
		})
	}
	return out
}

func toRunSummaryByID(row db.RunGetByIDJoinedJobsRow) RunSummary {
	return RunSummary{
		ID:           row.ID,
		RunKey:       row.RunKey,
		JobID:        row.JobID,
		JobKey:       row.JobKey,
		Status:       row.Status,
		TriggeredBy:  row.TriggeredBy,
		QueuedAt:     row.QueuedAt,
		StartedAt:    row.StartedAt,
		CompletedAt:  row.CompletedAt,
		ParentRunID:  row.ParentRunID,
		RerunStepKey: row.RerunStepKey,
		ErrorMessage: row.ErrorMessage,
	}
}

func toRunSummary(row db.RunGetManyJoinedJobsRow) RunSummary {
	return RunSummary{
		ID:           row.ID,
		RunKey:       row.RunKey,
		JobID:        row.JobID,
		JobKey:       row.JobKey,
		Status:       row.Status,
		TriggeredBy:  row.TriggeredBy,
		QueuedAt:     row.QueuedAt,
		StartedAt:    row.StartedAt,
		CompletedAt:  row.CompletedAt,
		ParentRunID:  row.ParentRunID,
		RerunStepKey: row.RerunStepKey,
		ErrorMessage: row.ErrorMessage,
	}
}

func toRunSteps(rows []db.RunStep) []RunStep {
	out := make([]RunStep, 0, len(rows))
	for _, row := range rows {
		out = append(out, RunStep{
			StepKey:      row.StepKey,
			Status:       row.Status,
			Attempt:      row.Attempt,
			StartedAt:    row.StartedAt,
			CompletedAt:  row.CompletedAt,
			DurationMs:   row.DurationMs,
			OutputJSON:   row.OutputJson,
			ErrorMessage: row.ErrorMessage,
			LogExcerpt:   row.LogExcerpt,
		})
	}
	return out
}

func toRunEvents(rows []db.RunEvent) []RunEvent {
	out := make([]RunEvent, 0, len(rows))
	for _, row := range rows {
		out = append(out, RunEvent{
			ID:            row.ID,
			StepKey:       row.StepKey,
			EventType:     row.EventType,
			Level:         row.Level,
			Message:       truncateForResponse(row.Message, maxRunEventMessageBytes),
			EventDataJSON: truncateForResponse(row.EventDataJson, maxRunEventDataJSONBytes),
			CreatedAt:     row.CreatedAt,
		})
	}
	return out
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

func nonEmpty(value, fallback string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return fallback
	}
	return trimmed
}

func truncateForResponse(value string, maxBytes int) string {
	if maxBytes <= 0 || len(value) <= maxBytes {
		return value
	}
	return fmt.Sprintf("%s… (%d chars truncated)", value[:maxBytes], len(value)-maxBytes)
}
