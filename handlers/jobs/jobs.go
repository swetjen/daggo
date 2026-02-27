package jobs

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"

	"daggo/db"
	"daggo/deps"
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

func (h *Handlers) JobsGetMany(ctx context.Context, req JobsGetManyRequest) (JobsGetManyResponse, int) {
	limit, offset := normalizePagination(req.Limit, req.Offset)
	rows, err := h.app.DB.JobGetMany(ctx, db.JobGetManyParams{Limit: limit, Offset: offset})
	if err != nil {
		return JobsGetManyResponse{Error: "failed to load jobs"}, rpc.StatusError
	}
	total, err := h.app.DB.JobCount(ctx)
	if err != nil {
		return JobsGetManyResponse{Error: "failed to count jobs"}, rpc.StatusError
	}

	out := make([]Job, 0, len(rows))
	for _, row := range rows {
		job, err := h.loadJobGraph(ctx, row)
		if err != nil {
			return JobsGetManyResponse{Error: "failed to load job graph"}, rpc.StatusError
		}
		out = append(out, job)
	}
	return JobsGetManyResponse{Data: out, Total: total}, rpc.StatusOK
}

func (h *Handlers) JobByKey(ctx context.Context, req JobByKeyRequest) (JobByKeyResponse, int) {
	if strings.TrimSpace(req.JobKey) == "" {
		return JobByKeyResponse{Error: "job_key is required"}, rpc.StatusInvalid
	}
	row, err := h.app.DB.JobGetByKey(ctx, req.JobKey)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return JobByKeyResponse{Error: "job not found"}, rpc.StatusInvalid
		}
		return JobByKeyResponse{Error: "failed to load job"}, rpc.StatusError
	}
	job, err := h.loadJobGraph(ctx, row)
	if err != nil {
		return JobByKeyResponse{Error: "failed to load job graph"}, rpc.StatusError
	}
	return JobByKeyResponse{Job: job}, rpc.StatusOK
}

func (h *Handlers) loadJobGraph(ctx context.Context, row db.Job) (Job, error) {
	nodes, err := h.app.DB.JobNodeGetManyByJobID(ctx, row.ID)
	if err != nil {
		return Job{}, err
	}
	edges, err := h.app.DB.JobEdgeGetManyByJobID(ctx, row.ID)
	if err != nil {
		return Job{}, err
	}
	schedules, err := h.app.DB.JobScheduleGetManyByJobID(ctx, row.ID)
	if err != nil {
		return Job{}, err
	}
	return Job{
		ID:                row.ID,
		JobKey:            row.JobKey,
		DisplayName:       row.DisplayName,
		Description:       row.Description,
		DefaultParamsJSON: row.DefaultParamsJson,
		Nodes:             toJobNodes(nodes),
		Edges:             toJobEdges(edges),
		Schedules:         toJobSchedules(schedules),
	}, nil
}

func toJobNodes(rows []db.JobNode) []JobNode {
	out := make([]JobNode, 0, len(rows))
	for _, row := range rows {
		node := JobNode{
			StepKey:     row.StepKey,
			DisplayName: row.DisplayName,
			Description: row.Description,
			Kind:        row.Kind,
			DependsOn:   []string{},
		}
		if strings.TrimSpace(row.MetadataJson) != "" {
			var payload struct {
				DependsOn []string `json:"depends_on"`
			}
			if err := json.Unmarshal([]byte(row.MetadataJson), &payload); err == nil {
				if payload.DependsOn == nil {
					node.DependsOn = []string{}
				} else {
					node.DependsOn = payload.DependsOn
				}
			}
		}
		out = append(out, node)
	}
	return out
}

func toJobEdges(rows []db.JobEdge) []JobEdge {
	out := make([]JobEdge, 0, len(rows))
	for _, row := range rows {
		out = append(out, JobEdge{FromStepKey: row.FromStepKey, ToStepKey: row.ToStepKey})
	}
	return out
}

func toJobSchedules(rows []db.JobSchedule) []JobSchedule {
	out := make([]JobSchedule, 0, len(rows))
	for _, row := range rows {
		out = append(out, JobSchedule{
			ScheduleKey: row.ScheduleKey,
			CronExpr:    row.CronExpr,
			Timezone:    row.Timezone,
			IsEnabled:   row.IsEnabled == 1,
			Description: row.Description,
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
