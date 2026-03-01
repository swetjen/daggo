package dag

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/swetjen/daggo/db"
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
			Op[NoInput, schedulerSourceOutput]("source", func(_ context.Context, _ NoInput) (schedulerSourceOutput, error) {
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
		Registry:      registry,
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

	state, err := queries.SchedulerScheduleStateGetByJobKeyScheduleKey(ctx, db.SchedulerScheduleStateGetByJobKeyScheduleKeyParams{
		JobKey:      job.Key,
		ScheduleKey: "every_minute",
	})
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

func TestSchedulerRunTick_UsesGeneratedScheduleKey(t *testing.T) {
	ctx := context.Background()
	queries, pool, err := db.NewTest()
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	t.Cleanup(func() {
		_ = pool.Close()
	})

	job := NewJob("scheduler_generated_key").
		Add(
			Op[NoInput, schedulerSourceOutput]("source", func(_ context.Context, _ NoInput) (schedulerSourceOutput, error) {
				return schedulerSourceOutput{Value: 1}, nil
			}),
		).
		AddSchedule(ScheduleDefinition{
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
		SchedulerKey:  "test-generated-key",
		TickInterval:  15 * time.Second,
		MaxDuePerTick: 4,
		Registry:      registry,
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
		t.Fatalf("expected triggered_by scheduler:every_minute, got %q", got)
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
			Op[NoInput, schedulerSourceOutput]("source", func(_ context.Context, _ NoInput) (schedulerSourceOutput, error) {
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
		Registry:      registry,
	})
	scheduler.nowFn = func() time.Time { return now }
	scheduler.runTick(ctx)

	_, err = queries.SchedulerScheduleStateUpsert(ctx, db.SchedulerScheduleStateUpsertParams{
		JobKey:         job.Key,
		ScheduleKey:    "every_minute",
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

func TestSchedulerRunTick_PrunesRemovedSchedulesButPreservesRuns(t *testing.T) {
	ctx := context.Background()
	queries, pool, err := db.NewTest()
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	t.Cleanup(func() {
		_ = pool.Close()
	})

	source := Op[NoInput, schedulerSourceOutput]("source", func(_ context.Context, _ NoInput) (schedulerSourceOutput, error) {
		return schedulerSourceOutput{Value: 1}, nil
	})

	jobWithSchedule := NewJob("scheduler_removed").
		Add(source).
		AddSchedule(ScheduleDefinition{
			Key:      "every_minute",
			CronExpr: "* * * * *",
			Timezone: "UTC",
			Enabled:  true,
		}).
		MustBuild()

	initialRegistry := NewRegistry()
	if err := initialRegistry.Register(jobWithSchedule); err != nil {
		t.Fatalf("register initial job: %v", err)
	}
	if err := initialRegistry.SyncToDB(ctx, queries, pool); err != nil {
		t.Fatalf("initial sync: %v", err)
	}

	now := time.Date(2026, time.February, 23, 19, 2, 2, 0, time.UTC)
	enqueuer := &recordingEnqueuer{}
	scheduler := NewScheduler(queries, pool, enqueuer, SchedulerOptions{
		SchedulerKey:  "test-scheduler-prune",
		TickInterval:  15 * time.Second,
		MaxDuePerTick: 4,
		Registry:      initialRegistry,
	})
	scheduler.nowFn = func() time.Time { return now }
	scheduler.runTick(ctx)

	runRows, err := queries.RunGetMany(ctx, db.RunGetManyParams{Limit: 20, Offset: 0})
	if err != nil {
		t.Fatalf("load runs after initial tick: %v", err)
	}
	if len(runRows) != 1 {
		t.Fatalf("expected one historical run before schedule removal, got %d", len(runRows))
	}

	states, err := queries.SchedulerScheduleStateGetMany(ctx)
	if err != nil {
		t.Fatalf("load scheduler states after initial tick: %v", err)
	}
	if len(states) != 1 {
		t.Fatalf("expected one scheduler state before schedule removal, got %d", len(states))
	}

	claims, err := queries.SchedulerScheduleRunGetDistinctMany(ctx)
	if err != nil {
		t.Fatalf("load scheduler claims after initial tick: %v", err)
	}
	if len(claims) != 1 {
		t.Fatalf("expected one scheduler claim before schedule removal, got %d", len(claims))
	}

	jobWithoutSchedule := NewJob("scheduler_removed").Add(source).MustBuild()
	nextRegistry := NewRegistry()
	if err := nextRegistry.Register(jobWithoutSchedule); err != nil {
		t.Fatalf("register updated job: %v", err)
	}
	if err := nextRegistry.SyncToDB(ctx, queries, pool); err != nil {
		t.Fatalf("updated sync: %v", err)
	}

	scheduler = NewScheduler(queries, pool, enqueuer, SchedulerOptions{
		SchedulerKey:  "test-scheduler-prune",
		TickInterval:  15 * time.Second,
		MaxDuePerTick: 4,
		Registry:      nextRegistry,
	})
	scheduler.nowFn = func() time.Time { return now.Add(2 * time.Minute) }
	scheduler.runTick(ctx)

	runRows, err = queries.RunGetMany(ctx, db.RunGetManyParams{Limit: 20, Offset: 0})
	if err != nil {
		t.Fatalf("load runs after schedule removal: %v", err)
	}
	if len(runRows) != 1 {
		t.Fatalf("expected historical run to be preserved after schedule removal, got %d", len(runRows))
	}

	states, err = queries.SchedulerScheduleStateGetMany(ctx)
	if err != nil {
		t.Fatalf("load scheduler states after schedule removal: %v", err)
	}
	if len(states) != 0 {
		t.Fatalf("expected scheduler state to be pruned after schedule removal, got %d", len(states))
	}

	claims, err = queries.SchedulerScheduleRunGetDistinctMany(ctx)
	if err != nil {
		t.Fatalf("load scheduler claims after schedule removal: %v", err)
	}
	if len(claims) != 0 {
		t.Fatalf("expected scheduler claims to be pruned after schedule removal, got %d", len(claims))
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
			Op[NoInput, schedulerSourceOutput]("source", func(_ context.Context, _ NoInput) (schedulerSourceOutput, error) {
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
		Registry:      registry,
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
			Op[NoInput, schedulerSourceOutput]("source", func(_ context.Context, _ NoInput) (schedulerSourceOutput, error) {
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
		Registry:      registry,
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
