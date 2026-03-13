package dag

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/swetjen/daggo/db"
)

func TestRegistrySyncToDBRetainsUnregisteredJobs(t *testing.T) {
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
		Add(Op[NoInput, struct{}]("step_a", func(_ context.Context, _ NoInput) (struct{}, error) {
			return struct{}{}, nil
		})).
		MustBuild()
	jobB := NewJob("job_b").
		Add(Op[NoInput, struct{}]("step_b", func(_ context.Context, _ NoInput) (struct{}, error) {
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
		t.Fatalf("job count before resync: %v", err)
	}
	if before != 2 {
		t.Fatalf("expected 2 jobs before resync, got %d", before)
	}

	next := NewRegistry()
	next.MustRegister(jobA)
	if err := next.SyncToDB(ctx, queries, pool); err != nil {
		t.Fatalf("resync: %v", err)
	}

	after, err := queries.JobCount(ctx)
	if err != nil {
		t.Fatalf("job count after resync: %v", err)
	}
	if after != 2 {
		t.Fatalf("expected 2 jobs after resync, got %d", after)
	}
	if _, err := queries.JobGetByKey(ctx, "job_b"); err != nil {
		t.Fatalf("expected job_b to be retained: %v", err)
	}
}

func TestRegistrySyncToDBUpsertsOpPartitions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	queries, pool, err := db.NewTest()
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	t.Cleanup(func() {
		_ = pool.Close()
	})

	job := NewJob("partition_sync").
		Add(
			Op[NoInput, struct{}]("step_a", func(_ context.Context, _ NoInput) (struct{}, error) {
				return struct{}{}, nil
			}).WithPartition(DailyFrom(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), WithEndOffset(-1))),
		).
		MustBuild()

	registry := NewRegistry()
	registry.MustRegister(job)
	if err := registry.SyncToDB(ctx, queries, pool); err != nil {
		t.Fatalf("sync registry: %v", err)
	}

	jobRow, err := queries.JobGetByKey(ctx, "partition_sync")
	if err != nil {
		t.Fatalf("load job row: %v", err)
	}
	definitions, err := queries.PartitionDefinitionGetManyByJobID(ctx, jobRow.ID)
	if err != nil {
		t.Fatalf("load partition definitions: %v", err)
	}
	if len(definitions) != 1 {
		t.Fatalf("expected one op partition definition, got %d", len(definitions))
	}
	if definitions[0].TargetKind != "op" || definitions[0].TargetKey != "step_a" {
		t.Fatalf("unexpected partition target: %+v", definitions[0])
	}
	keyCount, err := queries.PartitionKeyCountByDefinitionID(ctx, definitions[0].ID)
	if err != nil {
		t.Fatalf("count partition keys: %v", err)
	}
	if keyCount <= 0 {
		t.Fatalf("expected partition keys to be synced")
	}
}

func TestRegistrySyncToDBRejectsMultiplePartitionDomains(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	queries, pool, err := db.NewTest()
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	t.Cleanup(func() {
		_ = pool.Close()
	})

	stepA := Op[NoInput, struct{}]("step_a", func(_ context.Context, _ NoInput) (struct{}, error) {
		return struct{}{}, nil
	}).WithPartition(StringPartitions("a", "b"))
	stepB := Op[NoInput, struct{}]("step_b", func(_ context.Context, _ NoInput) (struct{}, error) {
		return struct{}{}, nil
	}).WithPartition(StringPartitions("x", "y", "z"))
	job := NewJob("mixed_partition_domains").Add(stepA, stepB).MustBuild()

	registry := NewRegistry()
	registry.MustRegister(job)
	err = registry.SyncToDB(ctx, queries, pool)
	if err == nil {
		t.Fatalf("expected mixed partition domains to fail sync")
	}
	if !strings.Contains(err.Error(), "multiple partition domains") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRegistrySyncToDBPartitionsAreIdempotent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	queries, pool, err := db.NewTest()
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	t.Cleanup(func() {
		_ = pool.Close()
	})

	job := NewJob("partition_sync_idempotent").
		Add(
			Op[NoInput, struct{}]("step_a", func(_ context.Context, _ NoInput) (struct{}, error) {
				return struct{}{}, nil
			}).WithPartition(StringPartitions("2026-01-01", "2026-01-02")),
		).
		MustBuild()

	registry := NewRegistry()
	registry.MustRegister(job)
	if err := registry.SyncToDB(ctx, queries, pool); err != nil {
		t.Fatalf("first sync: %v", err)
	}

	jobRow, err := queries.JobGetByKey(ctx, "partition_sync_idempotent")
	if err != nil {
		t.Fatalf("load job: %v", err)
	}
	definitions, err := queries.PartitionDefinitionGetManyByJobID(ctx, jobRow.ID)
	if err != nil {
		t.Fatalf("load definitions after first sync: %v", err)
	}
	if len(definitions) != 1 {
		t.Fatalf("expected one definition after first sync, got %d", len(definitions))
	}
	firstDefinitionID := definitions[0].ID

	if err := registry.SyncToDB(ctx, queries, pool); err != nil {
		t.Fatalf("second sync: %v", err)
	}
	definitions, err = queries.PartitionDefinitionGetManyByJobID(ctx, jobRow.ID)
	if err != nil {
		t.Fatalf("load definitions after second sync: %v", err)
	}
	if len(definitions) != 1 {
		t.Fatalf("expected one definition after second sync, got %d", len(definitions))
	}
	if definitions[0].ID != firstDefinitionID {
		t.Fatalf("expected definition id %d to be stable, got %d", firstDefinitionID, definitions[0].ID)
	}
	keys, err := queries.PartitionKeyGetManyByDefinitionID(ctx, db.PartitionKeyGetManyByDefinitionIDParams{
		PartitionDefinitionID: definitions[0].ID,
		Limit:                 10,
		Offset:                0,
	})
	if err != nil {
		t.Fatalf("load partition keys: %v", err)
	}
	if len(keys) != 2 {
		t.Fatalf("expected 2 partition keys, got %d", len(keys))
	}
}

func TestRegistrySyncToDBRemovesStaleOpPartitionDefinitions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	queries, pool, err := db.NewTest()
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	t.Cleanup(func() {
		_ = pool.Close()
	})

	partitioned := NewJob("partition_sync_stale").
		Add(
			Op[NoInput, struct{}]("step_a", func(_ context.Context, _ NoInput) (struct{}, error) {
				return struct{}{}, nil
			}).WithPartition(StringPartitions("a", "b")),
		).
		MustBuild()

	registry := NewRegistry()
	registry.MustRegister(partitioned)
	if err := registry.SyncToDB(ctx, queries, pool); err != nil {
		t.Fatalf("sync partitioned job: %v", err)
	}

	plain := NewJob("partition_sync_stale").
		Add(
			Op[NoInput, struct{}]("step_a", func(_ context.Context, _ NoInput) (struct{}, error) {
				return struct{}{}, nil
			}),
		).
		MustBuild()

	registry = NewRegistry()
	registry.MustRegister(plain)
	if err := registry.SyncToDB(ctx, queries, pool); err != nil {
		t.Fatalf("sync plain job: %v", err)
	}

	jobRow, err := queries.JobGetByKey(ctx, "partition_sync_stale")
	if err != nil {
		t.Fatalf("load job: %v", err)
	}
	definitions, err := queries.PartitionDefinitionGetManyByJobID(ctx, jobRow.ID)
	if err != nil {
		t.Fatalf("load definitions: %v", err)
	}
	for _, definition := range definitions {
		if definition.TargetKind == "op" {
			t.Fatalf("expected no op partition definitions after removal, got %+v", definition)
		}
	}
}
