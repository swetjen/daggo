package runs

import (
	"context"
	"testing"

	"github.com/swetjen/daggo/config"
	"github.com/swetjen/daggo/dag"
	"github.com/swetjen/daggo/db"
	"github.com/swetjen/daggo/deps"
	"github.com/swetjen/virtuous/rpc"
)

func TestRunCreateAllowedWhenJobSchedulingIsPaused(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	queries, pool, err := db.NewTest()
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	t.Cleanup(func() {
		_ = pool.Close()
	})

	job := dag.NewJob("paused_schedule_job").
		Add(dag.Op[dag.NoInput, disabledJobOutput]("source", func(_ context.Context, _ dag.NoInput) (disabledJobOutput, error) {
			return disabledJobOutput{Value: "ok"}, nil
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

	if _, err := app.Registry.SetJobSchedulingPaused(job.Key, true); err != nil {
		t.Fatalf("pause scheduling: %v", err)
	}

	handler := New(app)
	resp, status := handler.RunCreate(ctx, RunCreateRequest{
		JobKey:      job.Key,
		TriggeredBy: "ui",
	})
	if status != rpc.StatusOK {
		t.Fatalf("expected ok status, got %d", status)
	}
	if resp.Error != "" {
		t.Fatalf("expected no error, got %q", resp.Error)
	}
	if resp.Run.ID == 0 {
		t.Fatalf("expected created run")
	}
}

type disabledJobOutput struct {
	Value string `json:"value"`
}
