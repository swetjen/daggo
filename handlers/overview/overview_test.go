package overview

import (
	"context"
	"testing"
	"time"

	"github.com/swetjen/daggo/dag"
	"github.com/swetjen/daggo/db"
	"github.com/swetjen/daggo/deps"
	"github.com/swetjen/virtuous/rpc"
)

func TestOverviewGetReturnsWindowedRunsAndSchedules(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	queries, pool, err := db.NewTest()
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	t.Cleanup(func() {
		_ = pool.Close()
	})

	registry := dag.NewRegistry()
	jobA := dag.NewJob("overview_alpha").
		Add(dag.Op[dag.NoInput, struct{}]("noop", func(_ context.Context, _ dag.NoInput) (struct{}, error) {
			return struct{}{}, nil
		})).
		AddSchedule(dag.ScheduleDefinition{
			Key:      "alpha_hourly",
			CronExpr: "0 * * * *",
			Timezone: "UTC",
			Enabled:  true,
		}).
		MustBuild()
	jobB := dag.NewJob("overview_beta").
		Add(dag.Op[dag.NoInput, struct{}]("noop", func(_ context.Context, _ dag.NoInput) (struct{}, error) {
			return struct{}{}, nil
		})).
		AddSchedule(dag.ScheduleDefinition{
			Key:      "beta_hourly",
			CronExpr: "0 * * * *",
			Timezone: "UTC",
			Enabled:  true,
		}).
		MustBuild()
	if err := registry.Register(jobA); err != nil {
		t.Fatalf("register alpha: %v", err)
	}
	if err := registry.Register(jobB); err != nil {
		t.Fatalf("register beta: %v", err)
	}
	if err := registry.SyncToDB(ctx, queries, pool); err != nil {
		t.Fatalf("sync jobs: %v", err)
	}

	alphaRow, err := queries.JobGetByKey(ctx, jobA.Key)
	if err != nil {
		t.Fatalf("load alpha row: %v", err)
	}
	betaRow, err := queries.JobGetByKey(ctx, jobB.Key)
	if err != nil {
		t.Fatalf("load beta row: %v", err)
	}

	anchor := time.Date(2026, 3, 30, 12, 0, 0, 0, time.UTC)
	createOverviewRun(t, ctx, queries, alphaRow.ID, "alpha_in_window", "running", "scheduler:alpha_hourly", anchor.Add(-30*time.Minute))
	createOverviewRun(t, ctx, queries, alphaRow.ID, "alpha_old", "success", "manual", anchor.Add(-3*time.Hour))
	createOverviewRun(t, ctx, queries, betaRow.ID, "beta_in_window", "failed", "manual", anchor.Add(-15*time.Minute))

	handler := New(&deps.Deps{DB: queries, Pool: pool, Registry: registry})
	resp, status := handler.OverviewGet(ctx, OverviewGetRequest{
		WindowHours: 1,
		AnchorAt:    anchor.Format(time.RFC3339Nano),
		RunLimit:    100,
	})
	if status != rpc.StatusOK {
		t.Fatalf("expected ok status, got %d", status)
	}
	if len(resp.Jobs) != 2 {
		t.Fatalf("expected two jobs, got %d", len(resp.Jobs))
	}
	if len(resp.Runs) != 2 {
		t.Fatalf("expected two windowed runs, got %d", len(resp.Runs))
	}
	if resp.Stats.TotalRunsInWindow != 2 {
		t.Fatalf("expected total_runs_in_window=2, got %d", resp.Stats.TotalRunsInWindow)
	}
	if resp.Stats.RunningNow != 1 {
		t.Fatalf("expected running_now=1, got %d", resp.Stats.RunningNow)
	}
	if resp.Stats.FailedInWindow != 1 {
		t.Fatalf("expected failed_in_window=1, got %d", resp.Stats.FailedInWindow)
	}
	if resp.Stats.EnabledSchedules != 2 {
		t.Fatalf("expected enabled_schedules=2, got %d", resp.Stats.EnabledSchedules)
	}
}

func TestOverviewGetHonorsJobFilterAndPausedSchedules(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	queries, pool, err := db.NewTest()
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	t.Cleanup(func() {
		_ = pool.Close()
	})

	registry := dag.NewRegistry()
	job := dag.NewJob("mailbox_scan").
		WithDisplayName("Mailbox Scan").
		Add(dag.Op[dag.NoInput, struct{}]("noop", func(_ context.Context, _ dag.NoInput) (struct{}, error) {
			return struct{}{}, nil
		})).
		AddSchedule(dag.ScheduleDefinition{
			Key:      "scan_hourly",
			CronExpr: "0 * * * *",
			Timezone: "UTC",
			Enabled:  true,
		}).
		MustBuild()
	other := dag.NewJob("crm_sync").
		Add(dag.Op[dag.NoInput, struct{}]("noop", func(_ context.Context, _ dag.NoInput) (struct{}, error) {
			return struct{}{}, nil
		})).
		MustBuild()
	if err := registry.Register(job); err != nil {
		t.Fatalf("register job: %v", err)
	}
	if err := registry.Register(other); err != nil {
		t.Fatalf("register other job: %v", err)
	}
	if _, err := registry.SetJobSchedulingPaused(job.Key, true); err != nil {
		t.Fatalf("pause job scheduling: %v", err)
	}
	if err := registry.SyncToDB(ctx, queries, pool); err != nil {
		t.Fatalf("sync jobs: %v", err)
	}

	handler := New(&deps.Deps{DB: queries, Pool: pool, Registry: registry})
	resp, status := handler.OverviewGet(ctx, OverviewGetRequest{
		WindowHours: 6,
		JobQuery:    "mailbox",
		RunLimit:    50,
	})
	if status != rpc.StatusOK {
		t.Fatalf("expected ok status, got %d", status)
	}
	if len(resp.Jobs) != 1 {
		t.Fatalf("expected one filtered job, got %d", len(resp.Jobs))
	}
	if resp.Jobs[0].JobKey != job.Key {
		t.Fatalf("expected mailbox_scan, got %q", resp.Jobs[0].JobKey)
	}
	if len(resp.Jobs[0].Schedules) != 0 {
		t.Fatalf("expected paused job to expose no enabled schedules, got %+v", resp.Jobs[0].Schedules)
	}
}

func createOverviewRun(t *testing.T, ctx context.Context, queries *db.Queries, jobID int64, runKey, status, triggeredBy string, queuedAt time.Time) {
	t.Helper()

	queued := queuedAt.UTC().Format(time.RFC3339Nano)
	started := queuedAt.Add(10 * time.Second).UTC().Format(time.RFC3339Nano)
	completed := queuedAt.Add(20 * time.Second).UTC().Format(time.RFC3339Nano)
	if _, err := queries.RunCreate(ctx, db.RunCreateParams{
		RunKey:       runKey,
		JobID:        jobID,
		Status:       status,
		TriggeredBy:  triggeredBy,
		ParamsJson:   "{}",
		QueuedAt:     queued,
		StartedAt:    started,
		CompletedAt:  completed,
		ParentRunID:  0,
		RerunStepKey: "",
		ErrorMessage: "",
	}); err != nil {
		t.Fatalf("create run %s: %v", runKey, err)
	}
}
