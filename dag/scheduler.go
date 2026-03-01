package dag

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/swetjen/daggo/db"
)

type RunEnqueuer interface {
	EnqueueRun(runID int64)
}

type SchedulerOptions struct {
	SchedulerKey  string
	TickInterval  time.Duration
	MaxDuePerTick int
	DeployLock    *DeployLock
	Registry      *Registry
}

type Scheduler struct {
	queries  db.Store
	pool     *sql.DB
	enqueuer RunEnqueuer
	registry *Registry

	schedulerKey  string
	tickInterval  time.Duration
	maxDuePerTick int

	nowFn      func() time.Time
	cronParser cron.Parser
	startOnce  sync.Once
	deployLock *DeployLock
}

func NewScheduler(queries db.Store, pool *sql.DB, enqueuer RunEnqueuer, opts SchedulerOptions) *Scheduler {
	tickInterval := opts.TickInterval
	if tickInterval <= 0 {
		tickInterval = 15 * time.Second
	}
	maxDuePerTick := opts.MaxDuePerTick
	if maxDuePerTick <= 0 {
		maxDuePerTick = 24
	}
	schedulerKey := strings.TrimSpace(opts.SchedulerKey)
	if schedulerKey == "" {
		schedulerKey = "monolith"
	}
	return &Scheduler{
		queries:       queries,
		pool:          pool,
		enqueuer:      enqueuer,
		registry:      opts.Registry,
		schedulerKey:  schedulerKey,
		tickInterval:  tickInterval,
		maxDuePerTick: maxDuePerTick,
		nowFn:         time.Now,
		deployLock:    opts.DeployLock,
		cronParser: cron.NewParser(
			cron.Minute |
				cron.Hour |
				cron.Dom |
				cron.Month |
				cron.Dow |
				cron.Descriptor,
		),
	}
}

func (s *Scheduler) Start(ctx context.Context) {
	if s == nil || s.queries == nil || s.pool == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.startOnce.Do(func() {
		go s.loop(ctx)
	})
}

func (s *Scheduler) loop(ctx context.Context) {
	ticker := time.NewTicker(s.tickInterval)
	defer ticker.Stop()

	s.runTick(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.runTick(ctx)
		}
	}
}

func (s *Scheduler) runTick(ctx context.Context) {
	if s == nil || s.queries == nil {
		return
	}
	startedAt := s.nowFn().UTC()
	_ = s.upsertHeartbeat(ctx, startedAt, startedAt, time.Time{}, "")

	runErr := s.runTickWithNow(ctx, startedAt)
	if runErr != nil {
		slog.Error("daggo: scheduler tick failed", "scheduler_key", s.schedulerKey, "err", runErr)
	}

	completedAt := s.nowFn().UTC()
	errMessage := ""
	if runErr != nil {
		errMessage = runErr.Error()
	}
	_ = s.upsertHeartbeat(ctx, completedAt, startedAt, completedAt, errMessage)
}

func (s *Scheduler) runTickWithNow(ctx context.Context, now time.Time) error {
	if s.deployLock != nil && s.deployLock.IsDraining() {
		return nil
	}
	schedules, err := s.currentSchedules(ctx)
	if err != nil {
		return fmt.Errorf("load enabled schedules: %w", err)
	}
	if err := s.pruneInactiveScheduleRecords(ctx, schedules); err != nil {
		return fmt.Errorf("prune inactive schedules: %w", err)
	}
	if len(schedules) == 0 {
		return nil
	}

	var failures []string
	for _, schedule := range schedules {
		if err := s.processSchedule(ctx, schedule, now); err != nil {
			failures = append(failures, fmt.Sprintf("%s/%s: %v", schedule.JobKey, schedule.ScheduleKey, err))
		}
	}
	if len(failures) > 0 {
		return fmt.Errorf("scheduler processed with errors (%d): %s", len(failures), strings.Join(failures, "; "))
	}
	return nil
}

func (s *Scheduler) processSchedule(ctx context.Context, schedule runtimeSchedule, now time.Time) error {
	state, err := s.queries.SchedulerScheduleStateGetByJobKeyScheduleKey(ctx, db.SchedulerScheduleStateGetByJobKeyScheduleKeyParams{
		JobKey:      schedule.JobKey,
		ScheduleKey: schedule.ScheduleKey,
	})
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("load scheduler state: %w", err)
	}
	if err == sql.ErrNoRows {
		state = db.SchedulerScheduleState{
			JobKey:         schedule.JobKey,
			ScheduleKey:    schedule.ScheduleKey,
			LastCheckedAt:  "",
			LastEnqueuedAt: "",
			NextRunAt:      "",
		}
	}

	locationName := strings.TrimSpace(schedule.Timezone)
	if locationName == "" {
		locationName = "UTC"
	}
	location, err := time.LoadLocation(locationName)
	if err != nil {
		_ = s.persistScheduleState(ctx, schedule.JobKey, schedule.ScheduleKey, now.UTC(), state.LastEnqueuedAt, "")
		return fmt.Errorf("invalid timezone %q: %w", schedule.Timezone, err)
	}

	spec, err := s.cronParser.Parse(schedule.CronExpr)
	if err != nil {
		_ = s.persistScheduleState(ctx, schedule.JobKey, schedule.ScheduleKey, now.UTC(), state.LastEnqueuedAt, "")
		return fmt.Errorf("invalid cron expression %q: %w", schedule.CronExpr, err)
	}

	nowLocal := now.In(location)
	// Non-backfilling scheduler: only look back one tick.
	base := nowLocal.Add(-s.tickInterval)
	lastEnqueued := parseStoredTime(state.LastEnqueuedAt)
	if !lastEnqueued.IsZero() {
		lastEnqueuedLocal := lastEnqueued.In(location)
		if lastEnqueuedLocal.After(base) {
			base = lastEnqueuedLocal
		}
	}

	dueTimes, nextRun := s.resolveDueTimes(spec, base, nowLocal)
	lastEnqueuedAt := state.LastEnqueuedAt
	triggeredBy := fmt.Sprintf("scheduler:%s", schedule.ScheduleKey)
	for _, due := range dueTimes {
		dueUTC := due.UTC()
		dueAt := dueUTC.Format(time.RFC3339Nano)
		claims, err := s.queries.SchedulerScheduleRunsCreateIfAbsent(ctx, db.SchedulerScheduleRunsCreateIfAbsentParams{
			JobKey:       schedule.JobKey,
			ScheduleKey:  schedule.ScheduleKey,
			ScheduledFor: dueAt,
			RunKey:       "",
			TriggeredBy:  triggeredBy,
		})
		if err != nil {
			return fmt.Errorf("claim schedule run at %s: %w", dueAt, err)
		}
		if len(claims) == 0 {
			continue
		}

		run, err := s.createScheduledRun(ctx, schedule, dueUTC, triggeredBy)
		if err != nil {
			_ = s.queries.SchedulerScheduleRunDeleteByID(ctx, claims[0].ID)
			return fmt.Errorf("create scheduled run at %s: %w", dueAt, err)
		}
		if _, err := s.queries.SchedulerScheduleRunUpdateByID(ctx, db.SchedulerScheduleRunUpdateByIDParams{
			RunKey:      run.RunKey,
			TriggeredBy: triggeredBy,
			ID:          claims[0].ID,
		}); err != nil {
			slog.Error("daggo: failed to update schedule claim with run key", "job_key", schedule.JobKey, "schedule_key", schedule.ScheduleKey, "claim_id", claims[0].ID, "run_id", run.ID, "err", err)
		}
		if s.enqueuer != nil {
			s.enqueuer.EnqueueRun(run.ID)
		}
		lastEnqueuedAt = dueAt
	}

	nextRunAt := ""
	if !nextRun.IsZero() {
		nextRunAt = nextRun.UTC().Format(time.RFC3339Nano)
	}
	if err := s.persistScheduleState(ctx, schedule.JobKey, schedule.ScheduleKey, now.UTC(), lastEnqueuedAt, nextRunAt); err != nil {
		return fmt.Errorf("persist schedule state: %w", err)
	}
	return nil
}

func (s *Scheduler) resolveDueTimes(spec cron.Schedule, base, now time.Time) ([]time.Time, time.Time) {
	dueTimes := make([]time.Time, 0, 1)
	candidate := spec.Next(base)
	for !candidate.After(now) {
		dueTimes = append(dueTimes, candidate)
		if len(dueTimes) >= s.maxDuePerTick {
			break
		}
		candidate = spec.Next(candidate)
	}
	if len(dueTimes) >= s.maxDuePerTick && !candidate.After(now) {
		candidate = spec.Next(dueTimes[len(dueTimes)-1])
	}
	return dueTimes, candidate
}

func (s *Scheduler) persistScheduleState(ctx context.Context, jobKey, scheduleKey string, checkedAt time.Time, lastEnqueuedAt, nextRunAt string) error {
	_, err := s.queries.SchedulerScheduleStateUpsert(ctx, db.SchedulerScheduleStateUpsertParams{
		JobKey:         jobKey,
		ScheduleKey:    scheduleKey,
		LastCheckedAt:  checkedAt.Format(time.RFC3339Nano),
		LastEnqueuedAt: strings.TrimSpace(lastEnqueuedAt),
		NextRunAt:      strings.TrimSpace(nextRunAt),
	})
	return err
}

func (s *Scheduler) upsertHeartbeat(ctx context.Context, heartbeatAt, tickStartedAt, tickCompletedAt time.Time, lastError string) error {
	started := ""
	if !tickStartedAt.IsZero() {
		started = tickStartedAt.Format(time.RFC3339Nano)
	}
	completed := ""
	if !tickCompletedAt.IsZero() {
		completed = tickCompletedAt.Format(time.RFC3339Nano)
	}
	_, err := s.queries.SchedulerHeartbeatUpsert(ctx, db.SchedulerHeartbeatUpsertParams{
		SchedulerKey:        s.schedulerKey,
		LastHeartbeatAt:     heartbeatAt.Format(time.RFC3339Nano),
		LastTickStartedAt:   started,
		LastTickCompletedAt: completed,
		LastError:           strings.TrimSpace(lastError),
	})
	return err
}

func (s *Scheduler) createScheduledRun(
	ctx context.Context,
	schedule runtimeSchedule,
	scheduledFor time.Time,
	triggeredBy string,
) (db.Run, error) {
	nodes, err := s.queries.JobNodeGetManyByJobID(ctx, schedule.JobID)
	if err != nil {
		return db.Run{}, fmt.Errorf("load job nodes: %w", err)
	}
	if len(nodes) == 0 {
		return db.Run{}, fmt.Errorf("job %s has no nodes", schedule.JobKey)
	}

	tx, err := s.pool.BeginTx(ctx, nil)
	if err != nil {
		return db.Run{}, err
	}
	qtx := db.WithTx(s.queries, tx)

	paramsBytes, _ := json.Marshal(map[string]any{
		"scheduled_for": scheduledFor.UTC().Format(time.RFC3339Nano),
		"schedule_key":  schedule.ScheduleKey,
		"scheduler_key": s.schedulerKey,
	})
	now := s.nowFn().UTC().Format(time.RFC3339Nano)
	runKey := fmt.Sprintf("run_%d", s.nowFn().UTC().UnixNano())
	run, err := qtx.RunCreate(ctx, db.RunCreateParams{
		RunKey:       runKey,
		JobID:        schedule.JobID,
		Status:       "queued",
		TriggeredBy:  triggeredBy,
		ParamsJson:   string(paramsBytes),
		QueuedAt:     now,
		StartedAt:    "",
		CompletedAt:  "",
		ParentRunID:  0,
		RerunStepKey: "",
		ErrorMessage: "",
	})
	if err != nil {
		_ = tx.Rollback()
		return db.Run{}, err
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
			return db.Run{}, err
		}
	}

	eventBytes, _ := json.Marshal(map[string]any{
		"triggered_by":   run.TriggeredBy,
		"parent_run_id":  run.ParentRunID,
		"rerun_step_key": run.RerunStepKey,
		"schedule_key":   schedule.ScheduleKey,
		"scheduled_for":  scheduledFor.UTC().Format(time.RFC3339Nano),
	})
	if _, err := qtx.RunEventCreate(ctx, db.RunEventCreateParams{
		RunID:         run.ID,
		StepKey:       "",
		EventType:     "run_queued",
		Level:         "info",
		Message:       "run queued",
		EventDataJson: string(eventBytes),
	}); err != nil {
		_ = tx.Rollback()
		return db.Run{}, err
	}

	if err := tx.Commit(); err != nil {
		return db.Run{}, err
	}
	return run, nil
}

type runtimeSchedule struct {
	JobID       int64
	JobKey      string
	ScheduleKey string
	CronExpr    string
	Timezone    string
	Description string
}

func (s *Scheduler) currentSchedules(ctx context.Context) ([]runtimeSchedule, error) {
	if s == nil || s.registry == nil {
		return nil, nil
	}
	jobs := s.registry.Jobs()
	schedules := make([]runtimeSchedule, 0)
	for _, job := range jobs {
		jobRow, err := s.queries.JobGetByKey(ctx, job.Key)
		if err != nil {
			return nil, fmt.Errorf("load job %s: %w", job.Key, err)
		}
		for _, schedule := range job.Schedules {
			if !schedule.Enabled {
				continue
			}
			timezone := strings.TrimSpace(schedule.Timezone)
			if timezone == "" {
				timezone = "UTC"
			}
			schedules = append(schedules, runtimeSchedule{
				JobID:       jobRow.ID,
				JobKey:      job.Key,
				ScheduleKey: schedule.Key,
				CronExpr:    schedule.CronExpr,
				Timezone:    timezone,
				Description: schedule.Description,
			})
		}
	}
	return schedules, nil
}

func (s *Scheduler) pruneInactiveScheduleRecords(ctx context.Context, schedules []runtimeSchedule) error {
	active := make(map[string]struct{}, len(schedules))
	for _, schedule := range schedules {
		active[scheduleStateKey(schedule.JobKey, schedule.ScheduleKey)] = struct{}{}
	}

	states, err := s.queries.SchedulerScheduleStateGetMany(ctx)
	if err != nil {
		return err
	}
	for _, state := range states {
		if _, ok := active[scheduleStateKey(state.JobKey, state.ScheduleKey)]; ok {
			continue
		}
		if err := s.queries.SchedulerScheduleStateDeleteByJobKeyScheduleKey(ctx, db.SchedulerScheduleStateDeleteByJobKeyScheduleKeyParams{
			JobKey:      state.JobKey,
			ScheduleKey: state.ScheduleKey,
		}); err != nil {
			return err
		}
	}

	runKeys, err := s.queries.SchedulerScheduleRunGetDistinctMany(ctx)
	if err != nil {
		return err
	}
	for _, row := range runKeys {
		if _, ok := active[scheduleStateKey(row.JobKey, row.ScheduleKey)]; ok {
			continue
		}
		if err := s.queries.SchedulerScheduleRunsDeleteByJobKeyScheduleKey(ctx, db.SchedulerScheduleRunsDeleteByJobKeyScheduleKeyParams{
			JobKey:      row.JobKey,
			ScheduleKey: row.ScheduleKey,
		}); err != nil {
			return err
		}
	}
	return nil
}

func scheduleStateKey(jobKey, scheduleKey string) string {
	return jobKey + "\x00" + scheduleKey
}

func parseStoredTime(value string) time.Time {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return time.Time{}
	}
	parsed, err := time.Parse(time.RFC3339Nano, trimmed)
	if err == nil {
		return parsed
	}
	parsed, err = time.Parse(time.RFC3339, trimmed)
	if err == nil {
		return parsed
	}
	return time.Time{}
}
