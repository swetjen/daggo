package dag

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"daggo/db"
)

type recordingEnqueuer struct {
	mu     sync.Mutex
	runIDs []int64
}

func (r *recordingEnqueuer) EnqueueRun(runID int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.runIDs = append(r.runIDs, runID)
}

func (r *recordingEnqueuer) IDs() []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]int64, len(r.runIDs))
	copy(out, r.runIDs)
	return out
}

type schedulerSourceOutput struct {
	Value int `json:"value"`
}

func TestSchedulerRunTick_EnqueuesDueRunAndPersistsState(t *testing.T) {
	ctx := context.Background()
	queries, pool, err := db.NewTest()
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	t.Cleanup(func() {
		_ = pool.Close()
	})

	job := NewJob("scheduler_due").
		Add(
			Define[NoInput, schedulerSourceOutput]("source", func(_ context.Context, _ NoInput) (schedulerSourceOutput, error) {
				return schedulerSourceOutput{Value: 1}, nil
			}),
		).
		AddSchedule(ScheduleDefinition{
			Key:      "every_minute",
			CronExpr: "* * * * *",
			Timezone: "UTC",
			Enabled:  true,
		}).
		MustBuild()

	registry := NewRegistry()
	if err := registry.Register(job); err != nil {
		t.Fatalf("register job: %v", err)
	}
	if err := registry.SyncToDB(ctx, queries, pool); err != nil {
		t.Fatalf("sync registry: %v", err)
	}

	now := time.Date(2026, time.February, 23, 18, 6, 2, 0, time.UTC)
	enqueuer := &recordingEnqueuer{}
	scheduler := NewScheduler(queries, pool, enqueuer, SchedulerOptions{
		SchedulerKey:  "test-scheduler",
		TickInterval:  15 * time.Second,
		MaxDuePerTick: 4,
	})
	scheduler.nowFn = func() time.Time { return now }
	scheduler.runTick(ctx)

	runRows, err := queries.RunGetMany(ctx, db.RunGetManyParams{Limit: 20, Offset: 0})
	if err != nil {
		t.Fatalf("load runs: %v", err)
	}
	if len(runRows) != 1 {
		t.Fatalf("expected exactly one scheduled run, got %d", len(runRows))
	}
	if got := runRows[0].TriggeredBy; got != "scheduler:every_minute" {
		t.Fatalf("expected triggered_by scheduler value, got %q", got)
	}

	enqueued := enqueuer.IDs()
	if len(enqueued) != 1 || enqueued[0] != runRows[0].ID {
		t.Fatalf("expected one enqueued run ID matching created run, got %v", enqueued)
	}

	schedules, err := queries.SchedulerScheduleGetEnabledMany(ctx)
	if err != nil {
		t.Fatalf("load schedules: %v", err)
	}
	if len(schedules) != 1 {
		t.Fatalf("expected one enabled schedule, got %d", len(schedules))
	}
	state, err := queries.SchedulerScheduleStateGetByJobScheduleID(ctx, schedules[0].ID)
	if err != nil {
		t.Fatalf("load scheduler state: %v", err)
	}
	if state.LastCheckedAt == "" {
		t.Fatalf("expected non-empty last_checked_at")
	}
	if state.LastEnqueuedAt == "" {
		t.Fatalf("expected non-empty last_enqueued_at")
	}
	if state.NextRunAt == "" {
		t.Fatalf("expected non-empty next_run_at")
	}

	var heartbeatCount int
	if err := pool.QueryRowContext(ctx, "SELECT COUNT(1) FROM scheduler_heartbeats WHERE scheduler_key = ?", "test-scheduler").Scan(&heartbeatCount); err != nil {
		t.Fatalf("load scheduler heartbeat count: %v", err)
	}
	if heartbeatCount != 1 {
		t.Fatalf("expected one heartbeat row, got %d", heartbeatCount)
	}
}

func TestSchedulerRunTick_DedupesScheduledMoment(t *testing.T) {
	ctx := context.Background()
	queries, pool, err := db.NewTest()
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	t.Cleanup(func() {
		_ = pool.Close()
	})

	job := NewJob("scheduler_dedupe").
		Add(
			Define[NoInput, schedulerSourceOutput]("source", func(_ context.Context, _ NoInput) (schedulerSourceOutput, error) {
				return schedulerSourceOutput{Value: 1}, nil
			}),
		).
		AddSchedule(ScheduleDefinition{
			Key:      "every_minute",
			CronExpr: "* * * * *",
			Timezone: "UTC",
			Enabled:  true,
		}).
		MustBuild()

	registry := NewRegistry()
	if err := registry.Register(job); err != nil {
		t.Fatalf("register job: %v", err)
	}
	if err := registry.SyncToDB(ctx, queries, pool); err != nil {
		t.Fatalf("sync registry: %v", err)
	}

	now := time.Date(2026, time.February, 23, 19, 2, 2, 0, time.UTC)
	enqueuer := &recordingEnqueuer{}
	scheduler := NewScheduler(queries, pool, enqueuer, SchedulerOptions{
		SchedulerKey:  "test-scheduler-dedupe",
		TickInterval:  15 * time.Second,
		MaxDuePerTick: 4,
	})
	scheduler.nowFn = func() time.Time { return now }
	scheduler.runTick(ctx)

	schedules, err := queries.SchedulerScheduleGetEnabledMany(ctx)
	if err != nil {
		t.Fatalf("load schedules: %v", err)
	}
	if len(schedules) != 1 {
		t.Fatalf("expected one enabled schedule, got %d", len(schedules))
	}

	_, err = queries.SchedulerScheduleStateUpsert(ctx, db.SchedulerScheduleStateUpsertParams{
		JobScheduleID:  schedules[0].ID,
		LastCheckedAt:  now.UTC().Format(time.RFC3339Nano),
		LastEnqueuedAt: "",
		NextRunAt:      "",
	})
	if err != nil {
		t.Fatalf("reset scheduler state: %v", err)
	}

	scheduler.runTick(ctx)

	runRows, err := queries.RunGetMany(ctx, db.RunGetManyParams{Limit: 20, Offset: 0})
	if err != nil {
		t.Fatalf("load runs: %v", err)
	}
	if len(runRows) != 1 {
		t.Fatalf("expected one run after duplicate tick, got %d", len(runRows))
	}

	enqueued := enqueuer.IDs()
	if len(enqueued) != 1 {
		t.Fatalf("expected one enqueue call after duplicate tick, got %d", len(enqueued))
	}

	var claimCount int
	if err := pool.QueryRowContext(ctx, "SELECT COUNT(1) FROM scheduler_schedule_runs").Scan(&claimCount); err != nil {
		t.Fatalf("count schedule claims: %v", err)
	}
	if claimCount != 1 {
		t.Fatalf("expected one schedule claim row, got %d", claimCount)
	}
}

func TestSchedulerRunTick_DoesNotBackfillBurst(t *testing.T) {
	ctx := context.Background()
	queries, pool, err := db.NewTest()
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	t.Cleanup(func() {
		_ = pool.Close()
	})

	job := NewJob("scheduler_non_backfill").
		Add(
			Define[NoInput, schedulerSourceOutput]("source", func(_ context.Context, _ NoInput) (schedulerSourceOutput, error) {
				return schedulerSourceOutput{Value: 1}, nil
			}),
		).
		AddSchedule(ScheduleDefinition{
			Key:      "every_five",
			CronExpr: "*/5 * * * *",
			Timezone: "UTC",
			Enabled:  true,
		}).
		MustBuild()

	registry := NewRegistry()
	if err := registry.Register(job); err != nil {
		t.Fatalf("register job: %v", err)
	}
	if err := registry.SyncToDB(ctx, queries, pool); err != nil {
		t.Fatalf("sync registry: %v", err)
	}

	now := time.Date(2026, time.February, 23, 21, 27, 8, 0, time.UTC)
	enqueuer := &recordingEnqueuer{}
	scheduler := NewScheduler(queries, pool, enqueuer, SchedulerOptions{
		SchedulerKey:  "test-non-backfill",
		TickInterval:  15 * time.Second,
		MaxDuePerTick: 24,
	})
	scheduler.nowFn = func() time.Time { return now }
	scheduler.runTick(ctx)

	runRows, err := queries.RunGetMany(ctx, db.RunGetManyParams{Limit: 20, Offset: 0})
	if err != nil {
		t.Fatalf("load runs: %v", err)
	}
	if len(runRows) != 0 {
		t.Fatalf("expected no run at non-boundary tick, got %d", len(runRows))
	}
	if got := len(enqueuer.IDs()); got != 0 {
		t.Fatalf("expected no enqueued runs, got %d", got)
	}

	nextTick := time.Date(2026, time.February, 23, 21, 30, 2, 0, time.UTC)
	scheduler.nowFn = func() time.Time { return nextTick }
	scheduler.runTick(ctx)

	runRows, err = queries.RunGetMany(ctx, db.RunGetManyParams{Limit: 20, Offset: 0})
	if err != nil {
		t.Fatalf("load runs after boundary: %v", err)
	}
	if len(runRows) != 1 {
		t.Fatalf("expected one run at boundary, got %d", len(runRows))
	}
	if got := len(enqueuer.IDs()); got != 1 {
		t.Fatalf("expected one enqueued run at boundary, got %d", got)
	}
}

func TestSchedulerRunTick_SkipsRunCreationWhenDeployDrainActive(t *testing.T) {
	ctx := context.Background()
	queries, pool, err := db.NewTest()
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	t.Cleanup(func() {
		_ = pool.Close()
	})

	job := NewJob("scheduler_deploy_drain").
		Add(
			Define[NoInput, schedulerSourceOutput]("source", func(_ context.Context, _ NoInput) (schedulerSourceOutput, error) {
				return schedulerSourceOutput{Value: 1}, nil
			}),
		).
		AddSchedule(ScheduleDefinition{
			Key:      "every_minute",
			CronExpr: "* * * * *",
			Timezone: "UTC",
			Enabled:  true,
		}).
		MustBuild()

	registry := NewRegistry()
	if err := registry.Register(job); err != nil {
		t.Fatalf("register job: %v", err)
	}
	if err := registry.SyncToDB(ctx, queries, pool); err != nil {
		t.Fatalf("sync registry: %v", err)
	}

	lockPath := filepath.Join(t.TempDir(), "runtime", "WILL_DEPLOY")
	if err := EnsureDeployLockDir(lockPath); err != nil {
		t.Fatalf("ensure deploy lock dir: %v", err)
	}
	if err := os.WriteFile(lockPath, []byte("deploy"), 0o644); err != nil {
		t.Fatalf("write lock file: %v", err)
	}
	deployLock := NewDeployLock(lockPath, 5*time.Millisecond, 30*time.Second)

	now := time.Date(2026, time.February, 23, 18, 6, 2, 0, time.UTC)
	enqueuer := &recordingEnqueuer{}
	scheduler := NewScheduler(queries, pool, enqueuer, SchedulerOptions{
		SchedulerKey:  "test-deploy-drain",
		TickInterval:  15 * time.Second,
		MaxDuePerTick: 4,
		DeployLock:    deployLock,
	})
	scheduler.nowFn = func() time.Time { return now }
	scheduler.runTick(ctx)

	runRows, err := queries.RunGetMany(ctx, db.RunGetManyParams{Limit: 20, Offset: 0})
	if err != nil {
		t.Fatalf("load runs: %v", err)
	}
	if len(runRows) != 0 {
		t.Fatalf("expected no scheduled runs while deploy drain active, got %d", len(runRows))
	}
	if got := len(enqueuer.IDs()); got != 0 {
		t.Fatalf("expected no enqueued runs while deploy drain active, got %d", got)
	}
}
