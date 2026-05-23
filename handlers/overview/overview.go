package overview

import (
	"context"
	"database/sql"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/swetjen/daggo/dag"
	"github.com/swetjen/daggo/db"
	"github.com/swetjen/daggo/deps"
	"github.com/swetjen/virtuous/rpc"
)

const overviewPastRatio = 0.75

type Handlers struct {
	app *deps.Deps
}

func New(app *deps.Deps) *Handlers {
	return &Handlers{app: app}
}

type OverviewGetRequest struct {
	WindowHours int64  `json:"window_hours"`
	AnchorAt    string `json:"anchor_at"`
	JobQuery    string `json:"job_query"`
	RunLimit    int64  `json:"run_limit"`
}

type OverviewJobSchedule struct {
	ScheduleKey string `json:"schedule_key"`
	CronExpr    string `json:"cron_expr"`
	Timezone    string `json:"timezone"`
	Description string `json:"description"`
}

type OverviewJob struct {
	JobKey      string                `json:"job_key"`
	DisplayName string                `json:"display_name"`
	Schedules   []OverviewJobSchedule `json:"schedules"`
}

type OverviewRun struct {
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
}

type OverviewStats struct {
	RunningNow        int64 `json:"running_now"`
	FailedInWindow    int64 `json:"failed_in_window"`
	SuccessInWindow   int64 `json:"success_in_window"`
	EnabledSchedules  int64 `json:"enabled_schedules"`
	QuietJobs         int64 `json:"quiet_jobs"`
	TotalRunsInWindow int64 `json:"total_runs_in_window"`
}

type OverviewGetResponse struct {
	AnchorAt      string        `json:"anchor_at"`
	WindowStartAt string        `json:"window_start_at"`
	WindowEndAt   string        `json:"window_end_at"`
	Jobs          []OverviewJob `json:"jobs"`
	Runs          []OverviewRun `json:"runs"`
	Stats         OverviewStats `json:"stats"`
	Error         string        `json:"error,omitempty"`
}

type overviewJobState struct {
	job OverviewJob
	id  int64
}

func (h *Handlers) OverviewGet(ctx context.Context, req OverviewGetRequest) (OverviewGetResponse, int) {
	anchor := normalizeOverviewAnchor(req.AnchorAt)
	windowHours := normalizeOverviewWindowHours(req.WindowHours)
	windowStart, windowEnd := overviewWindowBounds(anchor, windowHours)
	runLimit := normalizeOverviewRunLimit(req.RunLimit)

	jobs := currentOverviewJobs(h.app, req.JobQuery)
	if len(jobs) == 0 {
		return OverviewGetResponse{
			AnchorAt:      anchor.Format(time.RFC3339Nano),
			WindowStartAt: windowStart.Format(time.RFC3339Nano),
			WindowEndAt:   windowEnd.Format(time.RFC3339Nano),
			Jobs:          []OverviewJob{},
			Runs:          []OverviewRun{},
			Stats:         OverviewStats{},
		}, rpc.StatusOK
	}

	states := make([]overviewJobState, 0, len(jobs))
	for _, definition := range jobs {
		row, err := h.app.DB.JobGetByKey(ctx, definition.Key)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			return OverviewGetResponse{Error: "failed to load jobs"}, rpc.StatusError
		}
		states = append(states, overviewJobState{
			id: row.ID,
			job: OverviewJob{
				JobKey:      definition.Key,
				DisplayName: nonEmpty(definition.DisplayName, definition.Key),
				Schedules:   overviewSchedulesForJob(h.app, definition),
			},
		})
	}
	if len(states) == 0 {
		return OverviewGetResponse{
			AnchorAt:      anchor.Format(time.RFC3339Nano),
			WindowStartAt: windowStart.Format(time.RFC3339Nano),
			WindowEndAt:   windowEnd.Format(time.RFC3339Nano),
			Jobs:          []OverviewJob{},
			Runs:          []OverviewRun{},
			Stats:         OverviewStats{},
		}, rpc.StatusOK
	}

	runs, err := h.loadOverviewRuns(ctx, states, windowStart, windowEnd, runLimit)
	if err != nil {
		return OverviewGetResponse{Error: "failed to load overview runs"}, rpc.StatusError
	}

	stats := buildOverviewStats(states, runs)
	return OverviewGetResponse{
		AnchorAt:      anchor.Format(time.RFC3339Nano),
		WindowStartAt: windowStart.Format(time.RFC3339Nano),
		WindowEndAt:   windowEnd.Format(time.RFC3339Nano),
		Jobs:          mapSlice(states, func(state overviewJobState) OverviewJob { return state.job }),
		Runs:          runs,
		Stats:         stats,
	}, rpc.StatusOK
}

func (h *Handlers) loadOverviewRuns(ctx context.Context, jobs []overviewJobState, windowStart, windowEnd time.Time, limit int64) ([]OverviewRun, error) {
	type cursor struct {
		job OverviewJob
		id  int64
		pos int64
	}

	active := make([]cursor, 0, len(jobs))
	for _, job := range jobs {
		active = append(active, cursor{job: job.job, id: job.id})
	}

	out := make([]OverviewRun, 0, limit)
	remaining := limit
	for remaining > 0 && len(active) > 0 {
		share := remaining / int64(len(active))
		if share <= 0 {
			share = 1
		}
		nextActive := make([]cursor, 0, len(active))
		progressed := false
		for _, state := range active {
			if remaining <= 0 {
				break
			}
			fetchLimit := minInt64(share, remaining)
			rows, err := h.app.DB.RunWindowGetMany(ctx, db.RunWindowParams{
				JobID:       state.id,
				WindowStart: windowStart.Format(time.RFC3339Nano),
				WindowEnd:   windowEnd.Format(time.RFC3339Nano),
				Limit:       fetchLimit,
				Offset:      state.pos,
			})
			if err != nil {
				return nil, err
			}
			got := int64(len(rows))
			if got > 0 {
				progressed = true
				out = append(out, toOverviewRuns(rows)...)
				state.pos += got
				remaining -= got
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

	sort.Slice(out, func(i, j int) bool {
		left := overviewRunFreshnessTime(out[i])
		right := overviewRunFreshnessTime(out[j])
		if left.Equal(right) {
			return out[i].ID > out[j].ID
		}
		return left.After(right)
	})
	return out, nil
}

func currentOverviewJobs(app *deps.Deps, query string) []dag.JobDefinition {
	if app == nil || app.Registry == nil {
		return nil
	}
	filter := strings.TrimSpace(strings.ToLower(query))
	jobs := append([]dag.JobDefinition(nil), app.Registry.Jobs()...)
	sort.Slice(jobs, func(i, j int) bool {
		left := nonEmpty(jobs[i].DisplayName, jobs[i].Key)
		right := nonEmpty(jobs[j].DisplayName, jobs[j].Key)
		return left < right
	})
	if filter == "" {
		return jobs
	}
	out := make([]dag.JobDefinition, 0, len(jobs))
	for _, job := range jobs {
		display := strings.ToLower(job.DisplayName)
		key := strings.ToLower(job.Key)
		if strings.Contains(display, filter) || strings.Contains(key, filter) {
			out = append(out, job)
		}
	}
	return out
}

func overviewSchedulesForJob(app *deps.Deps, job dag.JobDefinition) []OverviewJobSchedule {
	if app == nil || app.Registry == nil {
		return nil
	}
	if paused, ok := app.Registry.JobSchedulingPaused(job.Key); ok && paused {
		return nil
	}
	out := make([]OverviewJobSchedule, 0, len(job.Schedules))
	for _, schedule := range job.Schedules {
		if !schedule.Enabled {
			continue
		}
		timezone := strings.TrimSpace(schedule.Timezone)
		if timezone == "" {
			timezone = "UTC"
		}
		out = append(out, OverviewJobSchedule{
			ScheduleKey: schedule.Key,
			CronExpr:    schedule.CronExpr,
			Timezone:    timezone,
			Description: schedule.Description,
		})
	}
	return out
}

func normalizeOverviewAnchor(value string) time.Time {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return time.Now().UTC()
	}
	if parsed, err := time.Parse(time.RFC3339Nano, trimmed); err == nil {
		return parsed.UTC()
	}
	if parsed, err := time.Parse(time.RFC3339, trimmed); err == nil {
		return parsed.UTC()
	}
	return time.Now().UTC()
}

func normalizeOverviewWindowHours(hours int64) int64 {
	switch hours {
	case 1, 6, 12, 24:
		return hours
	default:
		return 6
	}
}

func normalizeOverviewRunLimit(limit int64) int64 {
	if limit <= 0 {
		return 1000
	}
	if limit > 2000 {
		return 2000
	}
	return limit
}

func overviewWindowBounds(anchor time.Time, hours int64) (time.Time, time.Time) {
	window := time.Duration(hours) * time.Hour
	past := time.Duration(float64(window) * overviewPastRatio)
	return anchor.Add(-past), anchor.Add(window - past)
}

func toOverviewRuns(rows []db.RunGetManyJoinedJobsRow) []OverviewRun {
	out := make([]OverviewRun, 0, len(rows))
	for _, row := range rows {
		out = append(out, OverviewRun{
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

func buildOverviewStats(jobs []overviewJobState, runs []OverviewRun) OverviewStats {
	runsByJob := make(map[string]int, len(jobs))
	stats := OverviewStats{}
	for _, run := range runs {
		runsByJob[run.JobKey]++
		stats.TotalRunsInWindow++
		switch normalizeRunStatus(run.Status) {
		case "running", "queued", "pending":
			stats.RunningNow++
		case "failed", "canceled":
			stats.FailedInWindow++
		case "success":
			stats.SuccessInWindow++
		}
	}
	for _, job := range jobs {
		stats.EnabledSchedules += int64(len(job.job.Schedules))
		if runsByJob[job.job.JobKey] == 0 {
			stats.QuietJobs++
		}
	}
	return stats
}

func normalizeRunStatus(status string) string {
	normalized := strings.TrimSpace(strings.ToLower(status))
	switch normalized {
	case "cancelled":
		return "canceled"
	case "in_progress":
		return "running"
	default:
		return normalized
	}
}

func overviewRunFreshnessTime(run OverviewRun) time.Time {
	for _, candidate := range []string{run.StartedAt, run.QueuedAt, run.CompletedAt} {
		if ts, err := time.Parse(time.RFC3339Nano, candidate); err == nil {
			return ts
		}
	}
	return time.Time{}
}

func nonEmpty(value, fallback string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return fallback
	}
	return trimmed
}

func minInt64(left, right int64) int64 {
	if left < right {
		return left
	}
	return right
}

func mapSlice[A any, B any](rows []A, mapper func(A) B) []B {
	out := make([]B, 0, len(rows))
	for _, row := range rows {
		out = append(out, mapper(row))
	}
	return out
}
