package runs

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/swetjen/daggo/dag"
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
	JobKey      string `json:"job_key"`
	Status      string `json:"status"`
	Search      string `json:"search"`
	QuickFilter string `json:"quick_filter"`
	WindowHours int64  `json:"window_hours"`
	Sort        string `json:"sort"`
	Limit       int64  `json:"limit"`
	Cursor      string `json:"cursor"`
}

type OverviewRunsGetManyRequest struct {
	Limit int64 `json:"limit"`
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
	Data       []RunSummary `json:"data"`
	Total      int64        `json:"total"`
	NextCursor string       `json:"next_cursor,omitempty"`
	Error      string       `json:"error,omitempty"`
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
	if h.app == nil || h.app.Registry == nil {
		return RunCreateResponse{Error: "job registry is unavailable"}, rpc.StatusError
	}
	if _, ok := h.app.Registry.JobByKey(jobKey); !ok {
		return RunCreateResponse{Error: "job not found"}, rpc.StatusInvalid
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
	if h.app == nil || h.app.Registry == nil {
		return RunCreateResponse{Error: "job registry is unavailable"}, rpc.StatusError
	}
	jobRow, err := h.app.DB.JobGetByID(ctx, sourceRun.JobID)
	if err != nil {
		return RunCreateResponse{Error: "failed to load source job"}, rpc.StatusError
	}
	if _, ok := h.app.Registry.JobByKey(jobRow.JobKey); !ok {
		return RunCreateResponse{Error: "job is not currently registered"}, rpc.StatusInvalid
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
	args, limit, err := h.buildCursorRunArgs(ctx, req)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return RunsGetManyResponse{Data: []RunSummary{}, Total: 0}, rpc.StatusOK
		}
		return RunsGetManyResponse{Error: err.Error()}, rpc.StatusInvalid
	}
	rows, err := h.app.DB.RunCursorGetMany(ctx, db.RunCursorParams{
		JobID:       args.JobID,
		Status:      args.Status,
		Search:      args.Search,
		QuickFilter: args.QuickFilter,
		WindowHours: args.WindowHours,
		Sort:        args.Sort,
		CursorAt:    args.CursorAt,
		CursorID:    args.CursorID,
		Limit:       limit + 1,
	})
	if err != nil {
		return RunsGetManyResponse{Error: "failed to load runs"}, rpc.StatusError
	}
	nextCursor := ""
	if int64(len(rows)) > limit {
		rows = rows[:limit]
		nextCursor = encodeRunsCursor(runsCursorToken{
			Sort:     args.Sort,
			CursorAt: runSummaryCursorAt(toRunSummary(rows[len(rows)-1])),
			CursorID: rows[len(rows)-1].ID,
		})
	}
	total, err := h.app.DB.RunFilteredCount(ctx, db.RunFilteredParams{
		JobID:       args.JobID,
		Status:      args.Status,
		Search:      args.Search,
		QuickFilter: args.QuickFilter,
		WindowHours: args.WindowHours,
	})
	if err != nil {
		return RunsGetManyResponse{Error: "failed to count runs"}, rpc.StatusError
	}
	data, err := h.decorateSummaries(ctx, toRunSummaries(rows))
	if err != nil {
		return RunsGetManyResponse{Error: "failed to load run steps"}, rpc.StatusError
	}
	return RunsGetManyResponse{Data: data, Total: total, NextCursor: nextCursor}, rpc.StatusOK
}

func (h *Handlers) OverviewRunsGetMany(ctx context.Context, req OverviewRunsGetManyRequest) (RunsGetManyResponse, int) {
	limit := normalizeOverviewLimit(req.Limit)
	jobs := overviewJobs(h.app)
	if limit <= 0 || len(jobs) == 0 {
		return RunsGetManyResponse{Data: []RunSummary{}, Total: 0}, rpc.StatusOK
	}

	type overviewJobState struct {
		jobID  int64
		offset int64
	}

	active := make([]overviewJobState, 0, len(jobs))
	for _, job := range jobs {
		row, err := h.app.DB.JobGetByKey(ctx, job.Key)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			return RunsGetManyResponse{Error: "failed to load jobs"}, rpc.StatusError
		}
		active = append(active, overviewJobState{jobID: row.ID})
	}
	if len(active) == 0 {
		return RunsGetManyResponse{Data: []RunSummary{}, Total: 0}, rpc.StatusOK
	}

	summaries := make([]RunSummary, 0, limit)
	remaining := limit
	for remaining > 0 && len(active) > 0 {
		share := remaining / int64(len(active))
		if share <= 0 {
			share = 1
		}

		nextActive := make([]overviewJobState, 0, len(active))
		progressed := false
		for _, state := range active {
			if remaining <= 0 {
				break
			}
			fetchLimit := minInt64(share, remaining)
			rows, err := h.app.DB.RunFilteredGetMany(ctx, db.RunFilteredParams{
				JobID:       state.jobID,
				Status:      "",
				Search:      "",
				QuickFilter: "",
				WindowHours: int64(0),
				Sort:        "newest",
				Limit:       fetchLimit,
				Offset:      state.offset,
			})
			if err != nil {
				return RunsGetManyResponse{Error: "failed to load overview runs"}, rpc.StatusError
			}
			got := int64(len(rows))
			if got > 0 {
				progressed = true
				summaries = append(summaries, toRunSummaries(rows)...)
				remaining -= got
				state.offset += got
			}
			if got == fetchLimit {
				nextActive = append(nextActive, state)
			}
		}
		if !progressed {
			break
		}
		active = nextActive
	}

	sort.Slice(summaries, func(i, j int) bool {
		left := runSummaryFreshnessTime(summaries[i])
		right := runSummaryFreshnessTime(summaries[j])
		if left.Equal(right) {
			return summaries[i].ID > summaries[j].ID
		}
		return left.After(right)
	})

	return RunsGetManyResponse{Data: summaries, Total: int64(len(summaries))}, rpc.StatusOK
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
	tx, err := h.app.Pool.BeginTx(ctx, nil)
	if err != nil {
		return RunSummary{}, err
	}
	qtx := db.WithTx(h.app.DB, tx)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	run, pendingSteps, err := dag.InsertQueuedRun(ctx, qtx, dag.RunInsertInput{
		JobID:        in.JobID,
		TriggeredBy:  in.TriggeredBy,
		ParamsJSON:   in.ParamsJSON,
		QueuedAt:     now,
		ParentRunID:  in.ParentRunID,
		RerunStepKey: in.RerunStepKey,
	})
	if err != nil {
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
		PendingSteps: int64(pendingSteps),
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

func normalizeCursorLimit(limit int64) int64 {
	if limit <= 0 {
		return 50
	}
	if limit > 200 {
		return 200
	}
	return limit
}

func normalizeOverviewLimit(limit int64) int64 {
	if limit <= 0 {
		return 1000
	}
	if limit > 1000 {
		return 1000
	}
	return limit
}

func normalizeRunStatusFilter(status string) string {
	switch strings.TrimSpace(strings.ToLower(status)) {
	case "", "all":
		return ""
	case "queued", "pending", "running", "success", "failed", "skipped", "canceled", "cancelled":
		return strings.TrimSpace(strings.ToLower(status))
	default:
		return ""
	}
}

func normalizeRunQuickFilter(filter string) string {
	switch strings.TrimSpace(strings.ToLower(filter)) {
	case "", "all":
		return ""
	case "backfills", "queued", "in_progress", "failed", "scheduled":
		return strings.TrimSpace(strings.ToLower(filter))
	default:
		return ""
	}
}

func normalizeRunSort(sortKey string) string {
	switch strings.TrimSpace(strings.ToLower(sortKey)) {
	case "oldest":
		return "oldest"
	default:
		return "newest"
	}
}

func overviewJobs(app *deps.Deps) []dag.JobDefinition {
	if app == nil || app.Registry == nil {
		return nil
	}
	out := append([]dag.JobDefinition(nil), app.Registry.Jobs()...)
	sort.Slice(out, func(i, j int) bool {
		return out[i].Key < out[j].Key
	})
	return out
}

type runsCursorToken struct {
	Sort     string `json:"sort"`
	CursorAt string `json:"cursor_at"`
	CursorID int64  `json:"cursor_id"`
}

func (h *Handlers) buildCursorRunArgs(ctx context.Context, req RunsGetManyRequest) (db.RunCursorParams, int64, error) {
	limit := normalizeCursorLimit(req.Limit)
	args := db.RunFilteredParams{
		JobID:       0,
		Status:      normalizeRunStatusFilter(req.Status),
		Search:      strings.TrimSpace(strings.ToLower(req.Search)),
		QuickFilter: normalizeRunQuickFilter(req.QuickFilter),
		WindowHours: maxInt64(req.WindowHours, 0),
		Sort:        normalizeRunSort(req.Sort),
		Limit:       limit,
	}
	if jobKey := strings.TrimSpace(req.JobKey); jobKey != "" {
		job, err := h.app.DB.JobGetByKey(ctx, jobKey)
		if err != nil {
			return db.RunCursorParams{}, 0, err
		}
		args.JobID = job.ID
	}
	cursor, err := decodeRunsCursor(req.Cursor)
	if err != nil {
		return db.RunCursorParams{}, 0, err
	}
	sortKey := normalizeRunSort(req.Sort)
	if cursor.Sort != "" && cursor.Sort == sortKey {
		args.Sort = cursor.Sort
	} else {
		cursor = runsCursorToken{}
	}
	return db.RunCursorParams{
		JobID:       args.JobID,
		Status:      args.Status,
		Search:      args.Search,
		QuickFilter: args.QuickFilter,
		WindowHours: args.WindowHours,
		Sort:        sortKey,
		CursorAt:    cursor.CursorAt,
		CursorID:    cursor.CursorID,
		Limit:       limit,
	}, limit, nil
}

func runSummaryFreshnessTime(run RunSummary) time.Time {
	for _, candidate := range []string{run.StartedAt, run.QueuedAt, run.CompletedAt} {
		if ts, err := time.Parse(time.RFC3339Nano, candidate); err == nil {
			return ts
		}
	}
	return time.Time{}
}

func runSummaryCursorAt(run RunSummary) string {
	for _, candidate := range []string{run.StartedAt, run.QueuedAt, run.CompletedAt} {
		if strings.TrimSpace(candidate) != "" {
			return candidate
		}
	}
	return ""
}

func encodeRunsCursor(cursor runsCursorToken) string {
	payload, err := json.Marshal(cursor)
	if err != nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(payload)
}

func decodeRunsCursor(value string) (runsCursorToken, error) {
	if strings.TrimSpace(value) == "" {
		return runsCursorToken{}, nil
	}
	payload, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(value))
	if err != nil {
		return runsCursorToken{}, errors.New("invalid cursor")
	}
	var cursor runsCursorToken
	if err := json.Unmarshal(payload, &cursor); err != nil {
		return runsCursorToken{}, errors.New("invalid cursor")
	}
	return cursor, nil
}

func maxInt64(left, right int64) int64 {
	if left > right {
		return left
	}
	return right
}

func minInt64(left, right int64) int64 {
	if left < right {
		return left
	}
	return right
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
