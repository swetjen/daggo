package jobs

import (
	"context"
	"database/sql"
	"errors"
	"strings"

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

type JobsGetManyRequest struct {
	Limit  int64 `json:"limit"`
	Offset int64 `json:"offset"`
}

type JobNode struct {
	StepKey     string   `json:"step_key"`
	DisplayName string   `json:"display_name"`
	Description string   `json:"description"`
	Kind        string   `json:"kind"`
	DependsOn   []string `json:"depends_on"`
}

type JobEdge struct {
	FromStepKey string `json:"from_step_key"`
	ToStepKey   string `json:"to_step_key"`
}

type JobSchedule struct {
	ScheduleKey string `json:"schedule_key"`
	CronExpr    string `json:"cron_expr"`
	Timezone    string `json:"timezone"`
	IsEnabled   bool   `json:"is_enabled"`
	Description string `json:"description"`
}

type Job struct {
	ID                int64         `json:"id"`
	JobKey            string        `json:"job_key"`
	DisplayName       string        `json:"display_name"`
	Description       string        `json:"description"`
	DefaultParamsJSON string        `json:"default_params_json"`
	SchedulingPaused  bool          `json:"scheduling_paused"`
	Nodes             []JobNode     `json:"nodes"`
	Edges             []JobEdge     `json:"edges"`
	Schedules         []JobSchedule `json:"schedules"`
}

type JobsGetManyResponse struct {
	Data  []Job  `json:"data"`
	Total int64  `json:"total"`
	Error string `json:"error,omitempty"`
}

type JobByKeyRequest struct {
	JobKey string `json:"job_key"`
}

type JobByKeyResponse struct {
	Job   Job    `json:"job"`
	Error string `json:"error,omitempty"`
}

type JobSchedulingUpdateRequest struct {
	JobKey           string `json:"job_key"`
	SchedulingPaused bool   `json:"scheduling_paused"`
}

type JobSchedulingUpdateResponse struct {
	Job   Job    `json:"job"`
	Error string `json:"error,omitempty"`
}

func (h *Handlers) JobsGetMany(ctx context.Context, req JobsGetManyRequest) (JobsGetManyResponse, int) {
	limit, offset := normalizePagination(req.Limit, req.Offset)
	definitions := currentJobs(h.app)
	total := int64(len(definitions))
	start := minInt(int(offset), len(definitions))
	end := minInt(start+int(limit), len(definitions))

	out := make([]Job, 0, end-start)
	for _, definition := range definitions[start:end] {
		job, err := h.loadCurrentJob(ctx, definition)
		if err != nil {
			return JobsGetManyResponse{Error: "failed to load jobs"}, rpc.StatusError
		}
		out = append(out, job)
	}
	return JobsGetManyResponse{Data: out, Total: total}, rpc.StatusOK
}

func (h *Handlers) JobByKey(ctx context.Context, req JobByKeyRequest) (JobByKeyResponse, int) {
	definition, ok := currentJobByKey(h.app, req.JobKey)
	if !ok {
		return JobByKeyResponse{Error: "job_key is required"}, rpc.StatusInvalid
	}
	job, err := h.loadCurrentJob(ctx, definition)
	if err != nil {
		return JobByKeyResponse{Error: "failed to load job"}, rpc.StatusError
	}
	return JobByKeyResponse{Job: job}, rpc.StatusOK
}

func (h *Handlers) JobSchedulingUpdate(ctx context.Context, req JobSchedulingUpdateRequest) (JobSchedulingUpdateResponse, int) {
	definition, err := setCurrentJobSchedulingPaused(h.app, req.JobKey, req.SchedulingPaused)
	if err != nil {
		if strings.Contains(err.Error(), "required") || strings.Contains(err.Error(), "not found") {
			return JobSchedulingUpdateResponse{Error: err.Error()}, rpc.StatusInvalid
		}
		return JobSchedulingUpdateResponse{Error: "failed to update scheduling state"}, rpc.StatusError
	}
	job, err := h.loadCurrentJob(ctx, definition)
	if err != nil {
		return JobSchedulingUpdateResponse{Error: "failed to load job"}, rpc.StatusError
	}
	return JobSchedulingUpdateResponse{Job: job}, rpc.StatusOK
}

func (h *Handlers) loadCurrentJob(ctx context.Context, definition dag.JobDefinition) (Job, error) {
	row, err := h.app.DB.JobGetByKey(ctx, definition.Key)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return Job{}, err
		}
		row = db.Job{}
	}
	schedulingPaused := currentJobSchedulingPaused(h.app, definition.Key)
	return Job{
		ID:                row.ID,
		JobKey:            definition.Key,
		DisplayName:       definition.DisplayName,
		Description:       definition.Description,
		DefaultParamsJSON: definition.DefaultParamsJSON,
		SchedulingPaused:  schedulingPaused,
		Nodes:             toJobNodes(definition.Steps),
		Edges:             toJobEdges(definition.Steps),
		Schedules:         toJobSchedules(schedulingPaused, definition.Schedules),
	}, nil
}

func toJobNodes(steps []dag.StepDefinition) []JobNode {
	out := make([]JobNode, 0, len(steps))
	for _, step := range steps {
		out = append(out, JobNode{
			StepKey:     step.Key,
			DisplayName: step.DisplayName,
			Description: step.Description,
			Kind:        "op",
			DependsOn:   append([]string(nil), step.DependsOn...),
		})
	}
	return out
}

func toJobEdges(steps []dag.StepDefinition) []JobEdge {
	var out []JobEdge
	for _, step := range steps {
		for _, dep := range step.DependsOn {
			out = append(out, JobEdge{FromStepKey: dep, ToStepKey: step.Key})
		}
	}
	return out
}

func toJobSchedules(schedulingPaused bool, rows []dag.ScheduleDefinition) []JobSchedule {
	out := make([]JobSchedule, 0, len(rows))
	for _, row := range rows {
		out = append(out, JobSchedule{
			ScheduleKey: row.Key,
			CronExpr:    row.CronExpr,
			Timezone:    row.Timezone,
			IsEnabled:   !schedulingPaused && row.Enabled,
			Description: row.Description,
		})
	}
	return out
}

func currentJobs(app *deps.Deps) []dag.JobDefinition {
	if app == nil || app.Registry == nil {
		return nil
	}
	return app.Registry.Jobs()
}

func currentJobByKey(app *deps.Deps, jobKey string) (dag.JobDefinition, bool) {
	if app == nil || app.Registry == nil {
		return dag.JobDefinition{}, false
	}
	trimmed := strings.TrimSpace(jobKey)
	if trimmed == "" {
		return dag.JobDefinition{}, false
	}
	return app.Registry.JobByKey(trimmed)
}

func currentJobSchedulingPaused(app *deps.Deps, jobKey string) bool {
	if app == nil || app.Registry == nil {
		return false
	}
	paused, ok := app.Registry.JobSchedulingPaused(jobKey)
	return ok && paused
}

func setCurrentJobSchedulingPaused(app *deps.Deps, jobKey string, paused bool) (dag.JobDefinition, error) {
	if app == nil || app.Registry == nil {
		return dag.JobDefinition{}, errors.New("job registry is unavailable")
	}
	trimmed := strings.TrimSpace(jobKey)
	if trimmed == "" {
		return dag.JobDefinition{}, errors.New("job_key is required")
	}
	return app.Registry.SetJobSchedulingPaused(trimmed, paused)
}

func minInt(left, right int) int {
	if left < right {
		return left
	}
	return right
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
