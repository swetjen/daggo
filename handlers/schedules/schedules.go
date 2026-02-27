package schedules

import (
	"context"

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

type SchedulesGetManyRequest struct {
	Limit  int64 `json:"limit"`
	Offset int64 `json:"offset"`
}

type Schedule struct {
	ID          int64  `json:"id"`
	JobID       int64  `json:"job_id"`
	JobKey      string `json:"job_key"`
	ScheduleKey string `json:"schedule_key"`
	CronExpr    string `json:"cron_expr"`
	Timezone    string `json:"timezone"`
	IsEnabled   bool   `json:"is_enabled"`
	Description string `json:"description"`
}

type SchedulesGetManyResponse struct {
	Data  []Schedule `json:"data"`
	Total int64      `json:"total"`
	Error string     `json:"error,omitempty"`
}

func (h *Handlers) SchedulesGetMany(ctx context.Context, req SchedulesGetManyRequest) (SchedulesGetManyResponse, int) {
	limit, offset := normalizePagination(req.Limit, req.Offset)
	rows, err := h.app.DB.JobScheduleGetMany(ctx, db.JobScheduleGetManyParams{Limit: limit, Offset: offset})
	if err != nil {
		return SchedulesGetManyResponse{Error: "failed to load schedules"}, rpc.StatusError
	}
	total, err := h.app.DB.JobScheduleCount(ctx)
	if err != nil {
		return SchedulesGetManyResponse{Error: "failed to count schedules"}, rpc.StatusError
	}
	return SchedulesGetManyResponse{
		Data:  toSchedules(rows),
		Total: total,
	}, rpc.StatusOK
}

func toSchedules(rows []db.JobScheduleGetManyRow) []Schedule {
	out := make([]Schedule, 0, len(rows))
	for _, row := range rows {
		out = append(out, Schedule{
			ID:          row.ID,
			JobID:       row.JobID,
			JobKey:      row.JobKey,
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
