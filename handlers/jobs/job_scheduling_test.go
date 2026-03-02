package jobs

import (
	"context"
	"testing"

	"github.com/swetjen/daggo/config"
	"github.com/swetjen/daggo/dag"
	"github.com/swetjen/daggo/db"
	"github.com/swetjen/daggo/deps"
	"github.com/swetjen/virtuous/rpc"
)

func TestJobSchedulingUpdateMutatesRegistryState(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	queries, pool, err := db.NewTest()
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	t.Cleanup(func() {
		_ = pool.Close()
	})

	job := dag.NewJob("toggle_job").
		Add(dag.Op[dag.NoInput, jobToggleOutput]("source", func(_ context.Context, _ dag.NoInput) (jobToggleOutput, error) {
			return jobToggleOutput{Value: "ok"}, nil
		})).
		AddSchedule(dag.ScheduleDefinition{
			Key:      "every_minute",
			CronExpr: "* * * * *",
			Timezone: "UTC",
			Enabled:  true,
		}).
		MustBuild()

	registry := dag.NewRegistry()
	if err := registry.Register(job); err != nil {
		t.Fatalf("register job: %v", err)
	}
	if err := registry.SyncToDB(ctx, queries, pool); err != nil {
		t.Fatalf("sync registry: %v", err)
	}

	app, err := deps.NewWithRegistry(ctx, config.Default(), queries, pool, registry)
	if err != nil {
		t.Fatalf("new deps: %v", err)
	}

	handler := New(app)
	resp, status := handler.JobSchedulingUpdate(ctx, JobSchedulingUpdateRequest{
		JobKey:           job.Key,
		SchedulingPaused: true,
	})
	if status != rpc.StatusOK {
		t.Fatalf("expected status ok, got %d", status)
	}
	if !resp.Job.SchedulingPaused {
		t.Fatalf("expected job response to show scheduling paused")
	}
	if len(resp.Job.Schedules) != 1 || resp.Job.Schedules[0].IsEnabled {
		t.Fatalf("expected paused scheduling to disable effective schedules, got %+v", resp.Job.Schedules)
	}

	updated, ok := app.Registry.JobSchedulingPaused(job.Key)
	if !ok {
		t.Fatalf("expected job scheduling state in registry")
	}
	if !updated {
		t.Fatalf("expected registry scheduling to be paused")
	}
}

type jobToggleOutput struct {
	Value string `json:"value"`
}
