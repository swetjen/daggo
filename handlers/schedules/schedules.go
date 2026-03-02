package schedules

import (
	"context"
	"hash/fnv"

	"github.com/swetjen/daggo/deps"
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
	_ = ctx
	limit, offset := normalizePagination(req.Limit, req.Offset)
	rows := currentSchedules(h.app)
	total := int64(len(rows))
	start := minInt(int(offset), len(rows))
	end := minInt(start+int(limit), len(rows))
	return SchedulesGetManyResponse{
		Data:  rows[start:end],
		Total: total,
	}, rpc.StatusOK
}

func currentSchedules(app *deps.Deps) []Schedule {
	if app == nil || app.Registry == nil {
		return nil
	}
	jobs := app.Registry.Jobs()
	out := make([]Schedule, 0)
	for _, job := range jobs {
		paused, _ := app.Registry.JobSchedulingPaused(job.Key)
		for _, schedule := range job.Schedules {
			out = append(out, Schedule{
				ID:          syntheticScheduleID(job.Key, schedule.Key),
				JobID:       syntheticScheduleID(job.Key, "job"),
				JobKey:      job.Key,
				ScheduleKey: schedule.Key,
				CronExpr:    schedule.CronExpr,
				Timezone:    schedule.Timezone,
				IsEnabled:   !paused && schedule.Enabled,
				Description: schedule.Description,
			})
		}
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

func minInt(left, right int) int {
	if left < right {
		return left
	}
	return right
}

func syntheticScheduleID(jobKey, suffix string) int64 {
	hasher := fnv.New64a()
	_, _ = hasher.Write([]byte(jobKey))
	_, _ = hasher.Write([]byte{0})
	_, _ = hasher.Write([]byte(suffix))
	return int64(hasher.Sum64() & 0x7fffffffffffffff)
}
