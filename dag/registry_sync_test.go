package dag

import (
	"context"
	"testing"

	"daggo/db"
)

func TestRegistrySyncToDBPrunesUnregisteredJobs(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	queries, pool, err := db.NewTest()
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	t.Cleanup(func() {
		_ = pool.Close()
	})

	jobA := NewJob("job_a").
		Add(Define[NoInput, struct{}]("step_a", func(_ context.Context, _ NoInput) (struct{}, error) {
			return struct{}{}, nil
		})).
		MustBuild()
	jobB := NewJob("job_b").
		Add(Define[NoInput, struct{}]("step_b", func(_ context.Context, _ NoInput) (struct{}, error) {
			return struct{}{}, nil
		})).
		MustBuild()

	initial := NewRegistry()
	initial.MustRegister(jobA)
	initial.MustRegister(jobB)
	if err := initial.SyncToDB(ctx, queries, pool); err != nil {
		t.Fatalf("initial sync: %v", err)
	}

	before, err := queries.JobCount(ctx)
	if err != nil {
		t.Fatalf("job count before prune: %v", err)
	}
	if before != 2 {
		t.Fatalf("expected 2 jobs before prune, got %d", before)
	}

	next := NewRegistry()
	next.MustRegister(jobA)
	if err := next.SyncToDB(ctx, queries, pool); err != nil {
		t.Fatalf("prune sync: %v", err)
	}

	after, err := queries.JobCount(ctx)
	if err != nil {
		t.Fatalf("job count after prune: %v", err)
	}
	if after != 1 {
		t.Fatalf("expected 1 job after prune, got %d", after)
	}
	if _, err := queries.JobGetByKey(ctx, "job_b"); err == nil {
		t.Fatalf("expected job_b to be pruned")
	}
}
