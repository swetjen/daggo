package runs

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/swetjen/daggo/dag"
	"github.com/swetjen/daggo/db"
	"github.com/swetjen/daggo/deps"
	"github.com/swetjen/virtuous/rpc"
)

func TestOverviewRunsGetManyRedistributesUnusedCapacity(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	queries, pool, registry := setupRunsHandlerTestDeps(t, ctx)

	jobA := mustRegisterRunsTestJob(t, ctx, queries, pool, registry, "overview_job_a")
	jobB := mustRegisterRunsTestJob(t, ctx, queries, pool, registry, "overview_job_b")

	base := time.Date(2026, 3, 29, 4, 0, 0, 0, time.UTC)
	createRunRow(t, ctx, queries, jobA.ID, "overview_a_only", "success", "manual", base.Add(1*time.Minute))
	createRunRow(t, ctx, queries, jobB.ID, "overview_b_1", "success", "manual", base.Add(2*time.Minute))
	createRunRow(t, ctx, queries, jobB.ID, "overview_b_2", "success", "manual", base.Add(3*time.Minute))
	createRunRow(t, ctx, queries, jobB.ID, "overview_b_3", "success", "manual", base.Add(4*time.Minute))
	createRunRow(t, ctx, queries, jobB.ID, "overview_b_4", "success", "manual", base.Add(5*time.Minute))

	handler := New(&deps.Deps{DB: queries, Pool: pool, Registry: registry})
	resp, status := handler.OverviewRunsGetMany(ctx, OverviewRunsGetManyRequest{Limit: 4})
	if status != rpc.StatusOK {
		t.Fatalf("expected ok status, got %d", status)
	}
	if resp.Total != 4 || len(resp.Data) != 4 {
		t.Fatalf("expected four overview runs, got total=%d len=%d", resp.Total, len(resp.Data))
	}

	byJob := map[string]int{}
	for _, run := range resp.Data {
		byJob[run.JobKey]++
	}
	if byJob["overview_job_a"] != 1 {
		t.Fatalf("expected one run from overview_job_a, got %+v", byJob)
	}
	if byJob["overview_job_b"] != 3 {
		t.Fatalf("expected three runs from overview_job_b after redistribution, got %+v", byJob)
	}
	if resp.Data[0].RunKey != "overview_b_4" || resp.Data[1].RunKey != "overview_b_3" {
		t.Fatalf("expected freshest runs first, got %+v", []string{resp.Data[0].RunKey, resp.Data[1].RunKey})
	}
}

func TestRunsGetManyAppliesServerFiltersAndPagination(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	queries, pool, registry := setupRunsHandlerTestDeps(t, ctx)

	jobA := mustRegisterRunsTestJob(t, ctx, queries, pool, registry, "filtered_job_a")
	jobB := mustRegisterRunsTestJob(t, ctx, queries, pool, registry, "filtered_job_b")

	base := time.Date(2026, 3, 29, 6, 0, 0, 0, time.UTC)
	createRunRow(t, ctx, queries, jobA.ID, "alpha-scheduled-old", "failed", "schedule:daily", base.Add(1*time.Minute))
	createRunRow(t, ctx, queries, jobA.ID, "alpha-scheduled-new", "failed", "schedule:hourly", base.Add(3*time.Minute))
	createRunRow(t, ctx, queries, jobA.ID, "alpha-manual", "success", "manual", base.Add(5*time.Minute))
	createRunRow(t, ctx, queries, jobB.ID, "beta-scheduled", "failed", "schedule:nightly", base.Add(7*time.Minute))

	handler := New(&deps.Deps{DB: queries, Pool: pool, Registry: registry})
	firstPage, status := handler.RunsGetMany(ctx, RunsGetManyRequest{
		JobKey:      "filtered_job_a",
		Status:      "failed",
		Search:      "alpha-scheduled",
		QuickFilter: "scheduled",
		Sort:        "oldest",
		Limit:       1,
	})
	if status != rpc.StatusOK {
		t.Fatalf("expected ok status, got %d", status)
	}
	if firstPage.Total != 2 {
		t.Fatalf("expected total 2 matching rows, got %d", firstPage.Total)
	}
	if len(firstPage.Data) != 1 || firstPage.Data[0].RunKey != "alpha-scheduled-old" {
		t.Fatalf("expected first page to return oldest filtered run, got %+v", firstPage.Data)
	}
	if firstPage.NextCursor == "" {
		t.Fatalf("expected next cursor for second page")
	}

	resp, status := handler.RunsGetMany(ctx, RunsGetManyRequest{
		JobKey:      "filtered_job_a",
		Status:      "failed",
		Search:      "alpha-scheduled",
		QuickFilter: "scheduled",
		Sort:        "oldest",
		Limit:       1,
		Cursor:      firstPage.NextCursor,
	})
	if status != rpc.StatusOK {
		t.Fatalf("expected second-page ok status, got %d", status)
	}
	if len(resp.Data) != 1 {
		t.Fatalf("expected one paged row, got %d", len(resp.Data))
	}
	if resp.Data[0].RunKey != "alpha-scheduled-new" {
		t.Fatalf("expected second-oldest filtered run, got %q", resp.Data[0].RunKey)
	}
	if resp.Data[0].JobKey != "filtered_job_a" {
		t.Fatalf("expected job filter to hold, got %q", resp.Data[0].JobKey)
	}
}

func setupRunsHandlerTestDeps(t *testing.T, _ context.Context) (*db.Queries, *sql.DB, *dag.Registry) {
	t.Helper()

	queries, pool, err := db.NewTest()
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	t.Cleanup(func() {
		_ = pool.Close()
	})

	registry := dag.NewRegistry()
	return queries, pool, registry
}

func mustRegisterRunsTestJob(t *testing.T, ctx context.Context, queries *db.Queries, pool *sql.DB, registry *dag.Registry, jobKey string) db.Job {
	t.Helper()

	definition := dag.NewJob(jobKey).
		Add(dag.Op[dag.NoInput, struct{}]("noop", func(_ context.Context, _ dag.NoInput) (struct{}, error) {
			return struct{}{}, nil
		})).
		MustBuild()
	if err := registry.Register(definition); err != nil {
		t.Fatalf("register %s: %v", jobKey, err)
	}
	if err := registry.SyncToDB(ctx, queries, pool); err != nil {
		t.Fatalf("sync jobs: %v", err)
	}
	row, err := queries.JobGetByKey(ctx, jobKey)
	if err != nil {
		t.Fatalf("load job row %s: %v", jobKey, err)
	}
	return row
}

func createRunRow(t *testing.T, ctx context.Context, queries *db.Queries, jobID int64, runKey, status, triggeredBy string, queuedAt time.Time) {
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
		ErrorMessage: fmt.Sprintf("%s-error", runKey),
	}); err != nil {
		t.Fatalf("create run %s: %v", runKey, err)
	}
}
