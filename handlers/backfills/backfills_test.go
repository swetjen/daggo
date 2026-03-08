package backfills

import (
	"context"
	"testing"
	"time"

	"github.com/swetjen/daggo/db"
	"github.com/swetjen/daggo/deps"
	"github.com/swetjen/virtuous/rpc"
)

func TestBackfillsGetManyByJobKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	queries, pool, err := db.NewTest()
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	t.Cleanup(func() {
		_ = pool.Close()
	})

	job, err := queries.JobUpsert(ctx, db.JobUpsertParams{
		JobKey:            "partition_job",
		DisplayName:       "Partition Job",
		Description:       "",
		DefaultParamsJson: "{}",
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}

	backfill, err := queries.BackfillCreate(ctx, db.BackfillCreateParams{
		BackfillKey:             "bf-test-1",
		JobID:                   job.ID,
		PartitionDefinitionID:   0,
		Status:                  "completed",
		SelectionMode:           "subset",
		SelectionJson:           `{"keys":["2026-01-01","2026-01-02"]}`,
		TriggeredBy:             "test",
		PolicyMode:              "multi_run",
		MaxPartitionsPerRun:     1,
		RequestedPartitionCount: 2,
		RequestedRunCount:       2,
		CompletedPartitionCount: 2,
		FailedPartitionCount:    0,
		ErrorMessage:            "",
		StartedAt:               time.Now().UTC().Format(time.RFC3339Nano),
		CompletedAt:             time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("create backfill: %v", err)
	}
	if _, err := queries.BackfillPartitionUpsert(ctx, db.BackfillPartitionUpsertParams{
		BackfillID:   backfill.ID,
		PartitionKey: "2026-01-01",
		Status:       "materialized",
		RunID:        1,
		ErrorMessage: "",
	}); err != nil {
		t.Fatalf("create backfill partition: %v", err)
	}
	if _, err := queries.BackfillPartitionUpsert(ctx, db.BackfillPartitionUpsertParams{
		BackfillID:   backfill.ID,
		PartitionKey: "2026-01-02",
		Status:       "failed_downstream",
		RunID:        2,
		ErrorMessage: "upstream failed",
	}); err != nil {
		t.Fatalf("create backfill partition: %v", err)
	}

	handler := New(&deps.Deps{DB: queries})
	response, status := handler.BackfillsGetMany(ctx, BackfillsGetManyRequest{
		JobKey: job.JobKey,
		Limit:  10,
		Offset: 0,
	})
	if status != rpc.StatusOK {
		t.Fatalf("expected status ok, got %d", status)
	}
	if response.Total != 1 {
		t.Fatalf("expected total 1, got %d", response.Total)
	}
	if len(response.Data) != 1 {
		t.Fatalf("expected one backfill summary, got %d", len(response.Data))
	}
	if !response.Data[0].IsComplete {
		t.Fatalf("expected backfill to be complete")
	}
	if response.Data[0].MaterializedCount != 1 || response.Data[0].FailedDownstreamCount != 1 {
		t.Fatalf("unexpected status counts: %+v", response.Data[0])
	}
}

func TestBackfillByKeyReturnsPartitions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	queries, pool, err := db.NewTest()
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	t.Cleanup(func() {
		_ = pool.Close()
	})

	job, err := queries.JobUpsert(ctx, db.JobUpsertParams{
		JobKey:            "partition_job_lookup",
		DisplayName:       "Partition Job Lookup",
		Description:       "",
		DefaultParamsJson: "{}",
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}

	backfill, err := queries.BackfillCreate(ctx, db.BackfillCreateParams{
		BackfillKey:             "bf-test-2",
		JobID:                   job.ID,
		PartitionDefinitionID:   0,
		Status:                  "running",
		SelectionMode:           "range",
		SelectionJson:           `{"start":"2026-01-01","end":"2026-01-02"}`,
		TriggeredBy:             "test",
		PolicyMode:              "single_run",
		MaxPartitionsPerRun:     2,
		RequestedPartitionCount: 2,
		RequestedRunCount:       1,
		CompletedPartitionCount: 0,
		FailedPartitionCount:    0,
		ErrorMessage:            "",
		StartedAt:               time.Now().UTC().Format(time.RFC3339Nano),
		CompletedAt:             "",
	})
	if err != nil {
		t.Fatalf("create backfill: %v", err)
	}
	if _, err := queries.BackfillPartitionUpsert(ctx, db.BackfillPartitionUpsertParams{
		BackfillID:   backfill.ID,
		PartitionKey: "2026-01-01",
		Status:       "requested",
		RunID:        1,
		ErrorMessage: "",
	}); err != nil {
		t.Fatalf("create backfill partition: %v", err)
	}

	handler := New(&deps.Deps{DB: queries})
	response, status := handler.BackfillByKey(ctx, BackfillByKeyRequest{
		BackfillKey:     backfill.BackfillKey,
		PartitionLimit:  10,
		PartitionOffset: 0,
	})
	if status != rpc.StatusOK {
		t.Fatalf("expected status ok, got %d", status)
	}
	if response.Backfill.BackfillKey != backfill.BackfillKey {
		t.Fatalf("expected backfill key %s, got %s", backfill.BackfillKey, response.Backfill.BackfillKey)
	}
	if response.PartitionsTotal != 1 || len(response.Partitions) != 1 {
		t.Fatalf("expected one partition result, got total=%d len=%d", response.PartitionsTotal, len(response.Partitions))
	}
	if response.Partitions[0].PartitionKey != "2026-01-01" {
		t.Fatalf("unexpected partition row: %+v", response.Partitions[0])
	}
}

func TestBackfillLaunchCreatesRunsAndTargets(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	queries, pool, err := db.NewTest()
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	t.Cleanup(func() {
		_ = pool.Close()
	})

	job, err := queries.JobUpsert(ctx, db.JobUpsertParams{
		JobKey:            "partition_job_launch",
		DisplayName:       "Partition Job Launch",
		Description:       "",
		DefaultParamsJson: "{}",
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	if _, err := queries.JobNodeCreate(ctx, db.JobNodeCreateParams{
		JobID:        job.ID,
		StepKey:      "step_one",
		DisplayName:  "Step One",
		Description:  "",
		Kind:         "op",
		MetadataJson: "{}",
		SortIndex:    0,
	}); err != nil {
		t.Fatalf("create job node: %v", err)
	}

	definition, err := queries.PartitionDefinitionUpsert(ctx, db.PartitionDefinitionUpsertParams{
		JobID:          job.ID,
		TargetKind:     "job",
		TargetKey:      "",
		DefinitionKind: "static",
		DefinitionJson: `{"keys":["2026-01-01","2026-01-02","2026-01-03"]}`,
		Enabled:        1,
	})
	if err != nil {
		t.Fatalf("create partition definition: %v", err)
	}
	for index, key := range []string{"2026-01-01", "2026-01-02", "2026-01-03"} {
		if _, err := queries.PartitionKeyUpsert(ctx, db.PartitionKeyUpsertParams{
			PartitionDefinitionID: definition.ID,
			PartitionKey:          key,
			SortIndex:             int64(index),
			IsActive:              1,
		}); err != nil {
			t.Fatalf("create partition key %s: %v", key, err)
		}
	}

	handler := New(&deps.Deps{DB: queries, Pool: pool})
	response, status := handler.BackfillLaunch(ctx, BackfillLaunchRequest{
		JobKey:              job.JobKey,
		SelectionMode:       "subset",
		PartitionKeys:       []string{"2026-01-01", "2026-01-03"},
		PolicyMode:          "multi_run",
		MaxPartitionsPerRun: 1,
		TriggeredBy:         "test",
	})
	if status != rpc.StatusOK {
		t.Fatalf("expected status ok, got %d (%s)", status, response.Error)
	}
	if len(response.RunIDs) != 2 {
		t.Fatalf("expected 2 runs, got %d", len(response.RunIDs))
	}
	if response.Backfill.BackfillKey == "" {
		t.Fatalf("expected backfill key")
	}

	targets, err := queries.RunPartitionTargetGetManyByBackfillKey(ctx, db.RunPartitionTargetGetManyByBackfillKeyParams{
		BackfillKey: response.Backfill.BackfillKey,
		Limit:       10,
		Offset:      0,
	})
	if err != nil {
		t.Fatalf("load run partition targets: %v", err)
	}
	if len(targets) != 2 {
		t.Fatalf("expected 2 run partition targets, got %d", len(targets))
	}

	backfill, err := queries.BackfillGetByKey(ctx, response.Backfill.BackfillKey)
	if err != nil {
		t.Fatalf("load backfill: %v", err)
	}
	requestedCount, err := queries.BackfillPartitionCountByBackfillIDAndStatus(ctx, db.BackfillPartitionCountByBackfillIDAndStatusParams{
		BackfillID: backfill.ID,
		Status:     backfillPartitionStatusRequested,
	})
	if err != nil {
		t.Fatalf("count requested partitions: %v", err)
	}
	if requestedCount != 2 {
		t.Fatalf("expected 2 requested partitions, got %d", requestedCount)
	}
}

func TestBackfillOperationalWorkflow(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	queries, pool, err := db.NewTest()
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	t.Cleanup(func() {
		_ = pool.Close()
	})

	job, err := queries.JobUpsert(ctx, db.JobUpsertParams{
		JobKey:            "partition_job_workflow",
		DisplayName:       "Partition Job Workflow",
		Description:       "",
		DefaultParamsJson: "{}",
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	if _, err := queries.JobNodeCreate(ctx, db.JobNodeCreateParams{
		JobID:        job.ID,
		StepKey:      "step_one",
		DisplayName:  "Step One",
		Description:  "",
		Kind:         "op",
		MetadataJson: "{}",
		SortIndex:    0,
	}); err != nil {
		t.Fatalf("create job node: %v", err)
	}

	definition, err := queries.PartitionDefinitionUpsert(ctx, db.PartitionDefinitionUpsertParams{
		JobID:          job.ID,
		TargetKind:     "job",
		TargetKey:      "",
		DefinitionKind: "static",
		DefinitionJson: `{"keys":["2026-02-01","2026-02-02","2026-02-03"]}`,
		Enabled:        1,
	})
	if err != nil {
		t.Fatalf("create partition definition: %v", err)
	}
	for index, key := range []string{"2026-02-01", "2026-02-02", "2026-02-03"} {
		if _, err := queries.PartitionKeyUpsert(ctx, db.PartitionKeyUpsertParams{
			PartitionDefinitionID: definition.ID,
			PartitionKey:          key,
			SortIndex:             int64(index),
			IsActive:              1,
		}); err != nil {
			t.Fatalf("create partition key %s: %v", key, err)
		}
	}

	handler := New(&deps.Deps{DB: queries, Pool: pool})
	launchResponse, status := handler.BackfillLaunch(ctx, BackfillLaunchRequest{
		JobKey:              job.JobKey,
		SelectionMode:       "range",
		RangeStartPartition: "2026-02-01",
		RangeEndPartition:   "2026-02-03",
		PolicyMode:          "multi_run",
		MaxPartitionsPerRun: 2,
		TriggeredBy:         "test",
	})
	if status != rpc.StatusOK {
		t.Fatalf("expected status ok, got %d (%s)", status, launchResponse.Error)
	}
	if len(launchResponse.RunIDs) != 2 {
		t.Fatalf("expected 2 run ids, got %d", len(launchResponse.RunIDs))
	}
	if launchResponse.Backfill.BackfillKey == "" {
		t.Fatalf("expected backfill key")
	}

	listResponse, listStatus := handler.BackfillsGetMany(ctx, BackfillsGetManyRequest{
		JobKey: job.JobKey,
		Limit:  10,
		Offset: 0,
	})
	if listStatus != rpc.StatusOK {
		t.Fatalf("expected list status ok, got %d", listStatus)
	}
	if listResponse.Total != 1 || len(listResponse.Data) != 1 {
		t.Fatalf("expected one listed backfill, got total=%d len=%d", listResponse.Total, len(listResponse.Data))
	}
	if listResponse.Data[0].QueuedCount != 3 {
		t.Fatalf("expected queued count 3, got %d", listResponse.Data[0].QueuedCount)
	}

	detailResponse, detailStatus := handler.BackfillByKey(ctx, BackfillByKeyRequest{
		BackfillKey:     launchResponse.Backfill.BackfillKey,
		PartitionLimit:  50,
		PartitionOffset: 0,
	})
	if detailStatus != rpc.StatusOK {
		t.Fatalf("expected detail status ok, got %d", detailStatus)
	}
	if detailResponse.PartitionsTotal != 3 || len(detailResponse.Partitions) != 3 {
		t.Fatalf("expected 3 partition rows, got total=%d len=%d", detailResponse.PartitionsTotal, len(detailResponse.Partitions))
	}

	backfillRow, err := queries.BackfillGetByKey(ctx, launchResponse.Backfill.BackfillKey)
	if err != nil {
		t.Fatalf("load backfill: %v", err)
	}
	if _, err := queries.BackfillPartitionUpdateStatusByBackfillIDAndPartitionKey(ctx, db.BackfillPartitionUpdateStatusByBackfillIDAndPartitionKeyParams{
		Status:       backfillPartitionStatusMaterialized,
		RunID:        launchResponse.RunIDs[0],
		ErrorMessage: "",
		BackfillID:   backfillRow.ID,
		PartitionKey: "2026-02-01",
	}); err != nil {
		t.Fatalf("update partition state: %v", err)
	}
	if _, err := queries.BackfillPartitionUpdateStatusByBackfillIDAndPartitionKey(ctx, db.BackfillPartitionUpdateStatusByBackfillIDAndPartitionKeyParams{
		Status:       backfillPartitionStatusMaterialized,
		RunID:        launchResponse.RunIDs[0],
		ErrorMessage: "",
		BackfillID:   backfillRow.ID,
		PartitionKey: "2026-02-02",
	}); err != nil {
		t.Fatalf("update partition state: %v", err)
	}
	if _, err := queries.BackfillPartitionUpdateStatusByBackfillIDAndPartitionKey(ctx, db.BackfillPartitionUpdateStatusByBackfillIDAndPartitionKeyParams{
		Status:       backfillPartitionStatusFailedDownstream,
		RunID:        launchResponse.RunIDs[1],
		ErrorMessage: "failed downstream",
		BackfillID:   backfillRow.ID,
		PartitionKey: "2026-02-03",
	}); err != nil {
		t.Fatalf("update partition state: %v", err)
	}

	if _, err := queries.BackfillUpdateStatus(ctx, db.BackfillUpdateStatusParams{
		Status:                  "completed",
		RequestedPartitionCount: 3,
		RequestedRunCount:       2,
		CompletedPartitionCount: 2,
		FailedPartitionCount:    1,
		ErrorMessage:            "",
		StartedAt:               backfillRow.StartedAt,
		CompletedAt:             time.Now().UTC().Format(time.RFC3339Nano),
		ID:                      backfillRow.ID,
	}); err != nil {
		t.Fatalf("update backfill status: %v", err)
	}

	finalListResponse, finalListStatus := handler.BackfillsGetMany(ctx, BackfillsGetManyRequest{
		JobKey: job.JobKey,
		Limit:  10,
		Offset: 0,
	})
	if finalListStatus != rpc.StatusOK {
		t.Fatalf("expected final list status ok, got %d", finalListStatus)
	}
	if len(finalListResponse.Data) != 1 {
		t.Fatalf("expected one final summary, got %d", len(finalListResponse.Data))
	}
	summary := finalListResponse.Data[0]
	if !summary.IsComplete {
		t.Fatalf("expected backfill to be complete")
	}
	if summary.MaterializedCount != 2 || summary.FailedDownstreamCount != 1 {
		t.Fatalf("unexpected summary counts: %+v", summary)
	}
}
