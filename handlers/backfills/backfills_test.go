package backfills

import (
	"context"
	"strings"
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

	definition, err := queries.PartitionDefinitionUpsert(ctx, db.PartitionDefinitionUpsertParams{
		JobID:          job.ID,
		TargetKind:     "op",
		TargetKey:      "step_one",
		DefinitionKind: "static",
		DefinitionJson: `{"keys":["2026-01-01","2026-01-02"]}`,
		Enabled:        1,
	})
	if err != nil {
		t.Fatalf("create partition definition: %v", err)
	}

	backfill, err := queries.BackfillCreate(ctx, db.BackfillCreateParams{
		BackfillKey:             "bf-test-1",
		JobID:                   job.ID,
		PartitionDefinitionID:   definition.ID,
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
	if !response.JobHasPartitions {
		t.Fatalf("expected job_has_partitions=true")
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

func TestBackfillsGetManyByJobKeyWithoutPartitionDefinition(t *testing.T) {
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
		JobKey:            "non_partitioned_job",
		DisplayName:       "Non Partitioned Job",
		Description:       "",
		DefaultParamsJson: "{}",
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
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
	if response.JobHasPartitions {
		t.Fatalf("expected job_has_partitions=false")
	}
	if response.Total != 0 || len(response.Data) != 0 {
		t.Fatalf("expected no backfills, got total=%d len=%d", response.Total, len(response.Data))
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
		TargetKind:     "op",
		TargetKey:      "step_one",
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
		TargetKind:     "op",
		TargetKey:      "step_one",
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

func TestResolveJobPartitionDefinitionMatrix(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name            string
		setup           func(t *testing.T, ctx context.Context, queries *db.Queries, job db.Job) int64
		wantFound       bool
		wantTargetKind  string
		wantTargetKey   string
		wantDefinition  int64
		wantErrorSubstr string
	}{
		{
			name: "op_only",
			setup: func(t *testing.T, ctx context.Context, queries *db.Queries, job db.Job) int64 {
				t.Helper()
				row, err := queries.PartitionDefinitionUpsert(ctx, db.PartitionDefinitionUpsertParams{
					JobID:          job.ID,
					TargetKind:     "op",
					TargetKey:      "step_one",
					DefinitionKind: "static",
					DefinitionJson: `{"keys":["a","b"]}`,
					Enabled:        1,
				})
				if err != nil {
					t.Fatalf("create op definition: %v", err)
				}
				return row.ID
			},
			wantFound:      true,
			wantTargetKind: "op",
			wantTargetKey:  "step_one",
		},
		{
			name: "legacy_job_only",
			setup: func(t *testing.T, ctx context.Context, queries *db.Queries, job db.Job) int64 {
				t.Helper()
				row, err := queries.PartitionDefinitionUpsert(ctx, db.PartitionDefinitionUpsertParams{
					JobID:          job.ID,
					TargetKind:     "job",
					TargetKey:      "",
					DefinitionKind: "static",
					DefinitionJson: `{"keys":["a","b"]}`,
					Enabled:        1,
				})
				if err != nil {
					t.Fatalf("create legacy definition: %v", err)
				}
				return row.ID
			},
			wantFound:      true,
			wantTargetKind: "job",
			wantTargetKey:  "",
		},
		{
			name: "both_prefers_op",
			setup: func(t *testing.T, ctx context.Context, queries *db.Queries, job db.Job) int64 {
				t.Helper()
				opRow, err := queries.PartitionDefinitionUpsert(ctx, db.PartitionDefinitionUpsertParams{
					JobID:          job.ID,
					TargetKind:     "op",
					TargetKey:      "step_one",
					DefinitionKind: "static",
					DefinitionJson: `{"keys":["a","b"]}`,
					Enabled:        1,
				})
				if err != nil {
					t.Fatalf("create op definition: %v", err)
				}
				if _, err := queries.PartitionDefinitionUpsert(ctx, db.PartitionDefinitionUpsertParams{
					JobID:          job.ID,
					TargetKind:     "job",
					TargetKey:      "",
					DefinitionKind: "static",
					DefinitionJson: `{"keys":["a","b"]}`,
					Enabled:        1,
				}); err != nil {
					t.Fatalf("create legacy definition: %v", err)
				}
				return opRow.ID
			},
			wantFound:      true,
			wantTargetKind: "op",
			wantTargetKey:  "step_one",
		},
		{
			name: "disabled_op_falls_back_to_legacy",
			setup: func(t *testing.T, ctx context.Context, queries *db.Queries, job db.Job) int64 {
				t.Helper()
				if _, err := queries.PartitionDefinitionUpsert(ctx, db.PartitionDefinitionUpsertParams{
					JobID:          job.ID,
					TargetKind:     "op",
					TargetKey:      "step_one",
					DefinitionKind: "static",
					DefinitionJson: `{"keys":["a","b"]}`,
					Enabled:        0,
				}); err != nil {
					t.Fatalf("create disabled op definition: %v", err)
				}
				legacy, err := queries.PartitionDefinitionUpsert(ctx, db.PartitionDefinitionUpsertParams{
					JobID:          job.ID,
					TargetKind:     "job",
					TargetKey:      "",
					DefinitionKind: "static",
					DefinitionJson: `{"keys":["a","b"]}`,
					Enabled:        1,
				})
				if err != nil {
					t.Fatalf("create legacy definition: %v", err)
				}
				return legacy.ID
			},
			wantFound:      true,
			wantTargetKind: "job",
			wantTargetKey:  "",
		},
		{
			name: "disabled_legacy_returns_none",
			setup: func(t *testing.T, ctx context.Context, queries *db.Queries, job db.Job) int64 {
				t.Helper()
				row, err := queries.PartitionDefinitionUpsert(ctx, db.PartitionDefinitionUpsertParams{
					JobID:          job.ID,
					TargetKind:     "job",
					TargetKey:      "",
					DefinitionKind: "static",
					DefinitionJson: `{"keys":["a","b"]}`,
					Enabled:        0,
				})
				if err != nil {
					t.Fatalf("create disabled legacy definition: %v", err)
				}
				return row.ID
			},
			wantFound: false,
		},
		{
			name: "mixed_op_domains_errors",
			setup: func(t *testing.T, ctx context.Context, queries *db.Queries, job db.Job) int64 {
				t.Helper()
				if _, err := queries.PartitionDefinitionUpsert(ctx, db.PartitionDefinitionUpsertParams{
					JobID:          job.ID,
					TargetKind:     "op",
					TargetKey:      "step_one",
					DefinitionKind: "static",
					DefinitionJson: `{"keys":["a","b"]}`,
					Enabled:        1,
				}); err != nil {
					t.Fatalf("create first op definition: %v", err)
				}
				if _, err := queries.PartitionDefinitionUpsert(ctx, db.PartitionDefinitionUpsertParams{
					JobID:          job.ID,
					TargetKind:     "op",
					TargetKey:      "step_two",
					DefinitionKind: "static",
					DefinitionJson: `{"keys":["x","y"]}`,
					Enabled:        1,
				}); err != nil {
					t.Fatalf("create second op definition: %v", err)
				}
				return 0
			},
			wantErrorSubstr: "different domains",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			queries, pool, err := db.NewTest()
			if err != nil {
				t.Fatalf("open test db: %v", err)
			}
			t.Cleanup(func() {
				_ = pool.Close()
			})

			job, err := queries.JobUpsert(ctx, db.JobUpsertParams{
				JobKey:            "resolve_job_partition_" + tc.name,
				DisplayName:       "resolve " + tc.name,
				Description:       "",
				DefaultParamsJson: "{}",
			})
			if err != nil {
				t.Fatalf("create job: %v", err)
			}
			wantID := tc.setup(t, ctx, queries, job)

			handler := New(&deps.Deps{DB: queries})
			got, found, err := handler.resolveJobPartitionDefinition(ctx, job.ID)
			if tc.wantErrorSubstr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrorSubstr) {
					t.Fatalf("expected error containing %q, got %v", tc.wantErrorSubstr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolve partition definition: %v", err)
			}
			if found != tc.wantFound {
				t.Fatalf("expected found=%v, got %v", tc.wantFound, found)
			}
			if !tc.wantFound {
				return
			}
			if got.TargetKind != tc.wantTargetKind {
				t.Fatalf("expected target kind %q, got %q", tc.wantTargetKind, got.TargetKind)
			}
			if got.TargetKey != tc.wantTargetKey {
				t.Fatalf("expected target key %q, got %q", tc.wantTargetKey, got.TargetKey)
			}
			if wantID > 0 && got.ID != wantID {
				t.Fatalf("expected definition id %d, got %d", wantID, got.ID)
			}
		})
	}
}

func TestNormalizeLaunchSelectionMatrix(t *testing.T) {
	t.Parallel()

	allKeys := []string{"a", "b", "c"}
	testCases := []struct {
		name            string
		req             BackfillLaunchRequest
		wantMode        string
		wantKeys        []string
		wantErrorSubstr string
	}{
		{
			name:     "defaults_to_subset",
			req:      BackfillLaunchRequest{PartitionKeys: []string{"a", "c"}},
			wantMode: "subset",
			wantKeys: []string{"a", "c"},
		},
		{
			name:     "all_mode",
			req:      BackfillLaunchRequest{SelectionMode: "all"},
			wantMode: "range",
			wantKeys: []string{"a", "b", "c"},
		},
		{
			name:            "unsupported_mode",
			req:             BackfillLaunchRequest{SelectionMode: "invalid"},
			wantErrorSubstr: "unsupported selection_mode",
		},
		{
			name:            "single_mode_missing_key",
			req:             BackfillLaunchRequest{SelectionMode: "single"},
			wantErrorSubstr: "requires key",
		},
		{
			name:            "range_mode_unknown_key",
			req:             BackfillLaunchRequest{SelectionMode: "range", RangeStartPartition: "a", RangeEndPartition: "z"},
			wantErrorSubstr: "not found",
		},
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			selection, err := normalizeLaunchSelection(allKeys, tc.req)
			if tc.wantErrorSubstr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrorSubstr) {
					t.Fatalf("expected error containing %q, got %v", tc.wantErrorSubstr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("normalize launch selection: %v", err)
			}
			if string(selection.Mode) != tc.wantMode {
				t.Fatalf("expected mode %q, got %q", tc.wantMode, selection.Mode)
			}
			if len(selection.Keys) != len(tc.wantKeys) {
				t.Fatalf("expected %d keys, got %d (%v)", len(tc.wantKeys), len(selection.Keys), selection.Keys)
			}
			for index := range tc.wantKeys {
				if selection.Keys[index] != tc.wantKeys[index] {
					t.Fatalf("expected keys %v, got %v", tc.wantKeys, selection.Keys)
				}
			}
		})
	}
}

func TestNormalizeLaunchPolicyMatrix(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name            string
		req             BackfillLaunchRequest
		wantMode        string
		wantMax         int
		wantErrorSubstr string
	}{
		{
			name:     "defaults_to_multi_run",
			req:      BackfillLaunchRequest{},
			wantMode: "multi_run",
			wantMax:  1,
		},
		{
			name:     "single_run",
			req:      BackfillLaunchRequest{PolicyMode: "single_run"},
			wantMode: "single_run",
			wantMax:  0,
		},
		{
			name:     "multi_run_normalizes_zero",
			req:      BackfillLaunchRequest{PolicyMode: "multi_run", MaxPartitionsPerRun: 0},
			wantMode: "multi_run",
			wantMax:  1,
		},
		{
			name:            "multi_run_cap_enforced",
			req:             BackfillLaunchRequest{PolicyMode: "multi_run", MaxPartitionsPerRun: maxPartitionsPerRun + 1},
			wantErrorSubstr: "max_partitions_per_run exceeds allowed limit",
		},
		{
			name:            "unsupported_mode",
			req:             BackfillLaunchRequest{PolicyMode: "unknown"},
			wantErrorSubstr: "unsupported policy_mode",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			policy, err := normalizeLaunchPolicy(tc.req)
			if tc.wantErrorSubstr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrorSubstr) {
					t.Fatalf("expected error containing %q, got %v", tc.wantErrorSubstr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("normalize launch policy: %v", err)
			}
			if string(policy.Mode) != tc.wantMode {
				t.Fatalf("expected mode %q, got %q", tc.wantMode, policy.Mode)
			}
			if policy.MaxPartitionsPerRun != tc.wantMax {
				t.Fatalf("expected max %d, got %d", tc.wantMax, policy.MaxPartitionsPerRun)
			}
		})
	}
}

func TestBackfillLaunchValidationErrors(t *testing.T) {
	testCases := []struct {
		name            string
		setup           func(t *testing.T, ctx context.Context, queries *db.Queries, job db.Job)
		req             BackfillLaunchRequest
		wantErrorSubstr string
	}{
		{
			name: "missing_partition_definition",
			setup: func(t *testing.T, ctx context.Context, queries *db.Queries, job db.Job) {
				t.Helper()
				createJobNode(t, ctx, queries, job.ID, "step_one")
			},
			req: BackfillLaunchRequest{
				SelectionMode: "single",
				PartitionKey:  "a",
				PolicyMode:    "multi_run",
			},
			wantErrorSubstr: "job has no partition definition",
		},
		{
			name: "partition_definition_without_keys",
			setup: func(t *testing.T, ctx context.Context, queries *db.Queries, job db.Job) {
				t.Helper()
				createJobNode(t, ctx, queries, job.ID, "step_one")
				createPartitionDefinition(t, ctx, queries, job.ID, "op", "step_one", "static", `{"keys":["a"]}`, 1)
			},
			req: BackfillLaunchRequest{
				SelectionMode: "single",
				PartitionKey:  "a",
				PolicyMode:    "multi_run",
			},
			wantErrorSubstr: "partition definition has no partition keys",
		},
		{
			name: "unsupported_selection_mode",
			setup: func(t *testing.T, ctx context.Context, queries *db.Queries, job db.Job) {
				t.Helper()
				definition := createPartitionDefinition(t, ctx, queries, job.ID, "op", "step_one", "static", `{"keys":["a"]}`, 1)
				createPartitionKeys(t, ctx, queries, definition.ID, []string{"a"})
				createJobNode(t, ctx, queries, job.ID, "step_one")
			},
			req: BackfillLaunchRequest{
				SelectionMode: "unknown",
				PolicyMode:    "multi_run",
			},
			wantErrorSubstr: "unsupported selection_mode",
		},
		{
			name: "unknown_selection_key",
			setup: func(t *testing.T, ctx context.Context, queries *db.Queries, job db.Job) {
				t.Helper()
				definition := createPartitionDefinition(t, ctx, queries, job.ID, "op", "step_one", "static", `{"keys":["a"]}`, 1)
				createPartitionKeys(t, ctx, queries, definition.ID, []string{"a"})
				createJobNode(t, ctx, queries, job.ID, "step_one")
			},
			req: BackfillLaunchRequest{
				SelectionMode: "single",
				PartitionKey:  "z",
				PolicyMode:    "multi_run",
			},
			wantErrorSubstr: "not found",
		},
		{
			name: "unsupported_policy_mode",
			setup: func(t *testing.T, ctx context.Context, queries *db.Queries, job db.Job) {
				t.Helper()
				definition := createPartitionDefinition(t, ctx, queries, job.ID, "op", "step_one", "static", `{"keys":["a"]}`, 1)
				createPartitionKeys(t, ctx, queries, definition.ID, []string{"a"})
				createJobNode(t, ctx, queries, job.ID, "step_one")
			},
			req: BackfillLaunchRequest{
				SelectionMode: "single",
				PartitionKey:  "a",
				PolicyMode:    "unsupported",
			},
			wantErrorSubstr: "unsupported policy_mode",
		},
		{
			name: "max_partitions_per_run_cap",
			setup: func(t *testing.T, ctx context.Context, queries *db.Queries, job db.Job) {
				t.Helper()
				definition := createPartitionDefinition(t, ctx, queries, job.ID, "op", "step_one", "static", `{"keys":["a"]}`, 1)
				createPartitionKeys(t, ctx, queries, definition.ID, []string{"a"})
				createJobNode(t, ctx, queries, job.ID, "step_one")
			},
			req: BackfillLaunchRequest{
				SelectionMode:       "single",
				PartitionKey:        "a",
				PolicyMode:          "multi_run",
				MaxPartitionsPerRun: maxPartitionsPerRun + 1,
			},
			wantErrorSubstr: "max_partitions_per_run exceeds allowed limit",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			queries, pool, err := db.NewTest()
			if err != nil {
				t.Fatalf("open test db: %v", err)
			}
			t.Cleanup(func() {
				_ = pool.Close()
			})

			job := createJob(t, ctx, queries, "launch_validation_"+tc.name)
			tc.setup(t, ctx, queries, job)

			handler := New(&deps.Deps{DB: queries, Pool: pool})
			req := tc.req
			req.JobKey = job.JobKey
			response, status := handler.BackfillLaunch(ctx, req)
			if status != rpc.StatusInvalid {
				t.Fatalf("expected invalid status, got %d (%s)", status, response.Error)
			}
			if !strings.Contains(response.Error, tc.wantErrorSubstr) {
				t.Fatalf("expected error containing %q, got %q", tc.wantErrorSubstr, response.Error)
			}
		})
	}
}

func createJob(t *testing.T, ctx context.Context, queries *db.Queries, jobKey string) db.Job {
	t.Helper()
	job, err := queries.JobUpsert(ctx, db.JobUpsertParams{
		JobKey:            jobKey,
		DisplayName:       jobKey,
		Description:       "",
		DefaultParamsJson: "{}",
	})
	if err != nil {
		t.Fatalf("create job %q: %v", jobKey, err)
	}
	return job
}

func createJobNode(t *testing.T, ctx context.Context, queries *db.Queries, jobID int64, stepKey string) {
	t.Helper()
	if _, err := queries.JobNodeCreate(ctx, db.JobNodeCreateParams{
		JobID:        jobID,
		StepKey:      stepKey,
		DisplayName:  stepKey,
		Description:  "",
		Kind:         "op",
		MetadataJson: "{}",
		SortIndex:    0,
	}); err != nil {
		t.Fatalf("create job node %q: %v", stepKey, err)
	}
}

func createPartitionDefinition(
	t *testing.T,
	ctx context.Context,
	queries *db.Queries,
	jobID int64,
	targetKind string,
	targetKey string,
	definitionKind string,
	definitionJSON string,
	enabled int64,
) db.PartitionDefinition {
	t.Helper()
	row, err := queries.PartitionDefinitionUpsert(ctx, db.PartitionDefinitionUpsertParams{
		JobID:          jobID,
		TargetKind:     targetKind,
		TargetKey:      targetKey,
		DefinitionKind: definitionKind,
		DefinitionJson: definitionJSON,
		Enabled:        enabled,
	})
	if err != nil {
		t.Fatalf("create partition definition %s/%s: %v", targetKind, targetKey, err)
	}
	return row
}

func createPartitionKeys(t *testing.T, ctx context.Context, queries *db.Queries, definitionID int64, keys []string) {
	t.Helper()
	for index, key := range keys {
		if _, err := queries.PartitionKeyUpsert(ctx, db.PartitionKeyUpsertParams{
			PartitionDefinitionID: definitionID,
			PartitionKey:          key,
			SortIndex:             int64(index),
			IsActive:              1,
		}); err != nil {
			t.Fatalf("create partition key %q: %v", key, err)
		}
	}
}
