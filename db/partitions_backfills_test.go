package db

import (
	"context"
	"fmt"
	"io/fs"
	"testing"
	"time"
)

func TestSQLiteMigrationIncludesPartitionsBackfills(t *testing.T) {
	queries, pool, err := NewTest()
	if err != nil {
		t.Fatalf("open sqlite test db: %v", err)
	}
	t.Cleanup(func() {
		_ = pool.Close()
	})
	if queries == nil {
		t.Fatalf("expected queries")
	}

	var total int64
	if err := pool.QueryRowContext(
		context.Background(),
		"SELECT COUNT(1) FROM schema_migrations WHERE name = ?",
		"sql/sqlite/schemas/006_partitions_backfills.sql",
	).Scan(&total); err != nil {
		t.Fatalf("query migration table: %v", err)
	}
	if total != 1 {
		t.Fatalf("expected migration to be applied once, got %d", total)
	}
}

func TestPartitionBackfillMigrationsAreEmbedded(t *testing.T) {
	sqlitePaths, err := fs.Glob(schemaFS, "sql/sqlite/schemas/*.sql")
	if err != nil {
		t.Fatalf("glob sqlite schemas: %v", err)
	}
	postgresPaths, err := fs.Glob(postgresSchemaFS, "sql/postgres/schemas/*.sql")
	if err != nil {
		t.Fatalf("glob postgres schemas: %v", err)
	}

	if !containsString(sqlitePaths, "sql/sqlite/schemas/006_partitions_backfills.sql") {
		t.Fatalf("missing sqlite partition migration in embed fs")
	}
	if !containsString(postgresPaths, "sql/postgres/schemas/006_partitions_backfills.sql") {
		t.Fatalf("missing postgres partition migration in embed fs")
	}
}

func TestPartitionsBackfillsSQLiteLifecycle(t *testing.T) {
	ctx := context.Background()
	queries, pool, err := NewTest()
	if err != nil {
		t.Fatalf("open sqlite test db: %v", err)
	}
	t.Cleanup(func() {
		_ = pool.Close()
	})

	job, err := queries.JobUpsert(ctx, JobUpsertParams{
		JobKey:            "partitions_job",
		DisplayName:       "Partitions Job",
		Description:       "test",
		DefaultParamsJson: "{}",
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}

	partitionDefinition, err := queries.PartitionDefinitionUpsert(ctx, PartitionDefinitionUpsertParams{
		JobID:          job.ID,
		TargetKind:     "job",
		TargetKey:      "",
		DefinitionKind: "static",
		DefinitionJson: `{"keys":["2026-01-01","2026-01-02","2026-01-03"]}`,
		Enabled:        1,
	})
	if err != nil {
		t.Fatalf("upsert partition definition: %v", err)
	}
	if partitionDefinition.JobID != job.ID {
		t.Fatalf("expected job_id %d, got %d", job.ID, partitionDefinition.JobID)
	}

	partitionKeys := []string{"2026-01-01", "2026-01-02", "2026-01-03"}
	for index, partitionKey := range partitionKeys {
		if _, err := queries.PartitionKeyUpsert(ctx, PartitionKeyUpsertParams{
			PartitionDefinitionID: partitionDefinition.ID,
			PartitionKey:          partitionKey,
			SortIndex:             int64(index),
			IsActive:              1,
		}); err != nil {
			t.Fatalf("upsert partition key %s: %v", partitionKey, err)
		}
	}

	subset, err := queries.PartitionKeyGetManyByDefinitionIDAndKeys(ctx, PartitionKeyGetManyByDefinitionIDAndKeysParams{
		PartitionDefinitionID: partitionDefinition.ID,
		PartitionKeys:         []string{"2026-01-01", "2026-01-03"},
	})
	if err != nil {
		t.Fatalf("get partition subset: %v", err)
	}
	if len(subset) != 2 {
		t.Fatalf("expected 2 subset partition keys, got %d", len(subset))
	}

	ranged, err := queries.PartitionKeyGetManyByDefinitionIDRange(ctx, PartitionKeyGetManyByDefinitionIDRangeParams{
		PartitionKey:          "2026-01-02",
		PartitionKey_2:        "2026-01-03",
		PartitionDefinitionID: partitionDefinition.ID,
	})
	if err != nil {
		t.Fatalf("get partition range: %v", err)
	}
	if len(ranged) != 2 {
		t.Fatalf("expected 2 ranged partition keys, got %d", len(ranged))
	}
	if ranged[0].PartitionKey != "2026-01-02" || ranged[1].PartitionKey != "2026-01-03" {
		t.Fatalf("unexpected ranged partition keys: %+v", ranged)
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	run, err := queries.RunCreate(ctx, RunCreateParams{
		RunKey:       fmt.Sprintf("run-%d", time.Now().UnixNano()),
		JobID:        job.ID,
		Status:       "queued",
		TriggeredBy:  "test",
		ParamsJson:   "{}",
		QueuedAt:     now,
		StartedAt:    "",
		CompletedAt:  "",
		ParentRunID:  0,
		RerunStepKey: "",
		ErrorMessage: "",
	})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	if _, err := queries.RunPartitionTargetUpsert(ctx, RunPartitionTargetUpsertParams{
		RunID:                 run.ID,
		PartitionDefinitionID: partitionDefinition.ID,
		SelectionMode:         "single",
		PartitionKey:          "2026-01-02",
		RangeStartKey:         "",
		RangeEndKey:           "",
		PartitionSubsetJson:   `[]`,
		TagsJson:              `{"daggo/partition":"2026-01-02"}`,
		BackfillKey:           "bf-001",
	}); err != nil {
		t.Fatalf("upsert run partition target: %v", err)
	}

	if _, err := queries.RunSystemTagUpsert(ctx, RunSystemTagUpsertParams{
		RunID:    run.ID,
		TagKey:   "daggo/partition",
		TagValue: "2026-01-02",
	}); err != nil {
		t.Fatalf("upsert run system tag: %v", err)
	}
	runTags, err := queries.RunSystemTagGetManyByTagKeyAndValue(ctx, RunSystemTagGetManyByTagKeyAndValueParams{
		TagKey:   "daggo/partition",
		TagValue: "2026-01-02",
		Limit:    10,
		Offset:   0,
	})
	if err != nil {
		t.Fatalf("get run system tags: %v", err)
	}
	if len(runTags) != 1 || runTags[0].RunID != run.ID {
		t.Fatalf("unexpected run system tags: %+v", runTags)
	}

	backfill, err := queries.BackfillCreate(ctx, BackfillCreateParams{
		BackfillKey:             "bf-001",
		JobID:                   job.ID,
		PartitionDefinitionID:   partitionDefinition.ID,
		Status:                  "requested",
		SelectionMode:           "subset",
		SelectionJson:           `{"keys":["2026-01-01","2026-01-02","2026-01-03"]}`,
		TriggeredBy:             "test",
		PolicyMode:              "multi_run",
		MaxPartitionsPerRun:     1,
		RequestedPartitionCount: 3,
		RequestedRunCount:       0,
		CompletedPartitionCount: 0,
		FailedPartitionCount:    0,
		ErrorMessage:            "",
		StartedAt:               "",
		CompletedAt:             "",
	})
	if err != nil {
		t.Fatalf("create backfill: %v", err)
	}

	for _, partitionKey := range partitionKeys {
		if _, err := queries.BackfillPartitionUpsert(ctx, BackfillPartitionUpsertParams{
			BackfillID:   backfill.ID,
			PartitionKey: partitionKey,
			Status:       "targeted",
			RunID:        0,
			ErrorMessage: "",
		}); err != nil {
			t.Fatalf("upsert backfill partition %s: %v", partitionKey, err)
		}
	}

	totalBackfillPartitions, err := queries.BackfillPartitionCountByBackfillID(ctx, backfill.ID)
	if err != nil {
		t.Fatalf("count backfill partitions: %v", err)
	}
	if totalBackfillPartitions != 3 {
		t.Fatalf("expected 3 backfill partitions, got %d", totalBackfillPartitions)
	}

	if _, err := queries.BackfillPartitionUpdateStatusByBackfillIDAndPartitionKey(ctx, BackfillPartitionUpdateStatusByBackfillIDAndPartitionKeyParams{
		Status:       "materialized",
		RunID:        run.ID,
		ErrorMessage: "",
		BackfillID:   backfill.ID,
		PartitionKey: "2026-01-02",
	}); err != nil {
		t.Fatalf("update backfill partition status: %v", err)
	}

	materializedCount, err := queries.BackfillPartitionCountByBackfillIDAndStatus(ctx, BackfillPartitionCountByBackfillIDAndStatusParams{
		BackfillID: backfill.ID,
		Status:     "materialized",
	})
	if err != nil {
		t.Fatalf("count materialized partitions: %v", err)
	}
	if materializedCount != 1 {
		t.Fatalf("expected 1 materialized partition, got %d", materializedCount)
	}

	updatedBackfill, err := queries.BackfillUpdateStatus(ctx, BackfillUpdateStatusParams{
		Status:                  "running",
		RequestedPartitionCount: 3,
		RequestedRunCount:       1,
		CompletedPartitionCount: 1,
		FailedPartitionCount:    0,
		ErrorMessage:            "",
		StartedAt:               now,
		CompletedAt:             "",
		ID:                      backfill.ID,
	})
	if err != nil {
		t.Fatalf("update backfill status: %v", err)
	}
	if updatedBackfill.Status != "running" {
		t.Fatalf("expected status running, got %s", updatedBackfill.Status)
	}
	if updatedBackfill.CompletedPartitionCount != 1 {
		t.Fatalf("expected completed partition count 1, got %d", updatedBackfill.CompletedPartitionCount)
	}

	targetsByBackfill, err := queries.RunPartitionTargetGetManyByBackfillKey(ctx, RunPartitionTargetGetManyByBackfillKeyParams{
		BackfillKey: "bf-001",
		Limit:       10,
		Offset:      0,
	})
	if err != nil {
		t.Fatalf("get run targets by backfill key: %v", err)
	}
	if len(targetsByBackfill) != 1 || targetsByBackfill[0].RunID != run.ID {
		t.Fatalf("unexpected run targets by backfill key: %+v", targetsByBackfill)
	}
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
