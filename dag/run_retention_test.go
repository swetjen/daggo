package dag

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/swetjen/daggo/db"
)

func TestRunRetentionRunOnceDeletesOldTerminalRunsAndQueueArtifacts(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	queries, pool, err := db.NewTest()
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	t.Cleanup(func() {
		_ = pool.Close()
	})

	now := time.Date(2026, time.March, 25, 12, 0, 0, 0, time.UTC)
	oldCompletedAt := now.AddDate(0, 0, -40)
	recentCompletedAt := now.AddDate(0, 0, -5)

	jobID := createRetentionJob(t, ctx, queries, "retention_old_job")
	queueID := createRetentionQueue(t, ctx, queries, "retention_old_queue")

	oldRunID := createRetentionRun(t, ctx, queries, jobID, "success", oldCompletedAt)
	recentRunID := createRetentionRun(t, ctx, queries, jobID, "success", recentCompletedAt)
	runningRunID := createRetentionRunWithState(t, ctx, queries, jobID, "running", now.AddDate(0, 0, -50), time.Time{})

	oldQueueItemID := createRetentionQueueItem(t, ctx, queries, queueID, "queue_item_old", "part_old", "complete", oldCompletedAt)
	linkRetentionQueueRun(t, ctx, queries, oldQueueItemID, jobID, oldRunID, "publish")

	retention := NewRunRetention(queries, pool, RunRetentionOptions{RunDays: 30, BatchSize: 1})
	retention.nowFn = func() time.Time { return now }

	result, err := retention.RunOnce(ctx)
	if err != nil {
		t.Fatalf("run retention once: %v", err)
	}
	if result.DeletedRuns != 1 {
		t.Fatalf("expected 1 deleted run, got %d", result.DeletedRuns)
	}
	if result.DeletedQueueItems != 1 {
		t.Fatalf("expected 1 deleted queue item, got %d", result.DeletedQueueItems)
	}

	if _, err := queries.RunGetByID(ctx, oldRunID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected old run to be deleted, got err=%v", err)
	}
	if _, err := queries.RunGetByID(ctx, recentRunID); err != nil {
		t.Fatalf("expected recent run to remain, got err=%v", err)
	}
	if _, err := queries.RunGetByID(ctx, runningRunID); err != nil {
		t.Fatalf("expected running run to remain, got err=%v", err)
	}
	if _, err := queries.QueueItemGetByIDJoinedQueues(ctx, oldQueueItemID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected old queue item to be deleted, got err=%v", err)
	}

	assertCount(t, ctx, pool, "SELECT COUNT(*) FROM run_steps WHERE run_id = ?", oldRunID, 0)
	assertCount(t, ctx, pool, "SELECT COUNT(*) FROM run_events WHERE run_id = ?", oldRunID, 0)
	assertCount(t, ctx, pool, "SELECT COUNT(*) FROM queue_item_runs WHERE queue_item_id = ?", oldQueueItemID, 0)
	assertCount(t, ctx, pool, "SELECT COUNT(*) FROM queue_item_step_metadata WHERE queue_item_id = ?", oldQueueItemID, 0)
}

func TestRunRetentionRunOnceKeepsQueueItemWhenLinkedRunRemains(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	queries, pool, err := db.NewTest()
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	t.Cleanup(func() {
		_ = pool.Close()
	})

	now := time.Date(2026, time.March, 25, 12, 0, 0, 0, time.UTC)
	oldCompletedAt := now.AddDate(0, 0, -45)
	recentCompletedAt := now.AddDate(0, 0, -2)

	oldJobID := createRetentionJob(t, ctx, queries, "retention_old_linked_job")
	recentJobID := createRetentionJob(t, ctx, queries, "retention_recent_linked_job")
	queueID := createRetentionQueue(t, ctx, queries, "retention_shared_queue")

	oldRunID := createRetentionRun(t, ctx, queries, oldJobID, "success", oldCompletedAt)
	recentRunID := createRetentionRun(t, ctx, queries, recentJobID, "success", recentCompletedAt)
	queueItemID := createRetentionQueueItem(t, ctx, queries, queueID, "queue_item_shared", "part_shared", "complete", recentCompletedAt)
	linkRetentionQueueRun(t, ctx, queries, queueItemID, oldJobID, oldRunID, "old_step")
	linkRetentionQueueRun(t, ctx, queries, queueItemID, recentJobID, recentRunID, "recent_step")

	retention := NewRunRetention(queries, pool, RunRetentionOptions{RunDays: 30, BatchSize: 8})
	retention.nowFn = func() time.Time { return now }

	result, err := retention.RunOnce(ctx)
	if err != nil {
		t.Fatalf("run retention once: %v", err)
	}
	if result.DeletedRuns != 1 {
		t.Fatalf("expected 1 deleted run, got %d", result.DeletedRuns)
	}
	if result.DeletedQueueItems != 0 {
		t.Fatalf("expected shared queue item to remain, got %d deleted queue items", result.DeletedQueueItems)
	}

	if _, err := queries.RunGetByID(ctx, oldRunID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected old linked run to be deleted, got err=%v", err)
	}
	if _, err := queries.RunGetByID(ctx, recentRunID); err != nil {
		t.Fatalf("expected recent linked run to remain, got err=%v", err)
	}
	if _, err := queries.QueueItemGetByIDJoinedQueues(ctx, queueItemID); err != nil {
		t.Fatalf("expected queue item to remain, got err=%v", err)
	}

	linkedRuns, err := queries.QueueItemRunGetManyByQueueItemIDJoinedRuns(ctx, queueItemID)
	if err != nil {
		t.Fatalf("load linked runs: %v", err)
	}
	if len(linkedRuns) != 1 {
		t.Fatalf("expected 1 linked run after purge, got %d", len(linkedRuns))
	}
	if linkedRuns[0].RunID != recentRunID {
		t.Fatalf("expected recent run %d to remain linked, got %d", recentRunID, linkedRuns[0].RunID)
	}

	metadataRows, err := queries.QueueItemStepMetadataGetManyByQueueItemID(ctx, queueItemID)
	if err != nil {
		t.Fatalf("load queue metadata: %v", err)
	}
	if len(metadataRows) != 1 {
		t.Fatalf("expected 1 metadata row after purge, got %d", len(metadataRows))
	}
	if metadataRows[0].RunID != recentRunID {
		t.Fatalf("expected metadata to remain linked to recent run %d, got %d", recentRunID, metadataRows[0].RunID)
	}
}

func TestRunRetentionRunOnceDisabledIsNoop(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	queries, pool, err := db.NewTest()
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	t.Cleanup(func() {
		_ = pool.Close()
	})

	now := time.Date(2026, time.March, 25, 12, 0, 0, 0, time.UTC)
	jobID := createRetentionJob(t, ctx, queries, "retention_disabled_job")
	oldRunID := createRetentionRun(t, ctx, queries, jobID, "success", now.AddDate(0, 0, -90))

	retention := NewRunRetention(queries, pool, RunRetentionOptions{RunDays: 0, BatchSize: 4})
	retention.nowFn = func() time.Time { return now }

	result, err := retention.RunOnce(ctx)
	if err != nil {
		t.Fatalf("run retention once: %v", err)
	}
	if result.DeletedRuns != 0 || result.DeletedQueueItems != 0 {
		t.Fatalf("expected disabled retention to do nothing, got %+v", result)
	}
	if _, err := queries.RunGetByID(ctx, oldRunID); err != nil {
		t.Fatalf("expected old run to remain when disabled, got err=%v", err)
	}
}

func TestRunRetentionStartRunsImmediatePass(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	queries, pool, err := db.NewTest()
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	t.Cleanup(func() {
		_ = pool.Close()
	})

	now := time.Date(2026, time.March, 25, 12, 0, 0, 0, time.UTC)
	jobID := createRetentionJob(t, ctx, queries, "retention_start_job")
	oldRunID := createRetentionRun(t, ctx, queries, jobID, "success", now.AddDate(0, 0, -60))

	retention := NewRunRetention(queries, pool, RunRetentionOptions{
		RunDays:   30,
		Interval:  time.Hour,
		BatchSize: 4,
	})
	retention.nowFn = func() time.Time { return now }
	retention.Start(ctx)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, err := queries.RunGetByID(ctx, oldRunID)
		if errors.Is(err, sql.ErrNoRows) {
			return
		}
		if err != nil {
			t.Fatalf("load run during retention start: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("expected retention worker startup pass to delete old run %d", oldRunID)
}

func createRetentionJob(t *testing.T, ctx context.Context, queries db.Store, key string) int64 {
	t.Helper()

	job, err := queries.JobUpsert(ctx, db.JobUpsertParams{
		JobKey:            key,
		DisplayName:       key,
		Description:       "retention test job",
		DefaultParamsJson: "{}",
	})
	if err != nil {
		t.Fatalf("create job %s: %v", key, err)
	}
	return job.ID
}

func createRetentionQueue(t *testing.T, ctx context.Context, queries db.Store, key string) int64 {
	t.Helper()

	queueRow, err := queries.QueueUpsert(ctx, db.QueueUpsertParams{
		QueueKey:             key,
		DisplayName:          key,
		Description:          "retention test queue",
		RoutePath:            "",
		LoadMode:             "poll",
		LoadPollEverySeconds: 0,
	})
	if err != nil {
		t.Fatalf("create queue %s: %v", key, err)
	}
	return queueRow.ID
}

func createRetentionRun(t *testing.T, ctx context.Context, queries db.Store, jobID int64, status string, completedAt time.Time) int64 {
	t.Helper()
	return createRetentionRunWithState(t, ctx, queries, jobID, status, completedAt.Add(-2*time.Minute), completedAt)
}

func createRetentionRunWithState(t *testing.T, ctx context.Context, queries db.Store, jobID int64, status string, startedAt, completedAt time.Time) int64 {
	t.Helper()

	queuedAt := startedAt
	if queuedAt.IsZero() {
		queuedAt = time.Now().UTC()
	}

	run, err := queries.RunCreate(ctx, db.RunCreateParams{
		RunKey:       fmt.Sprintf("retention_run_%d", time.Now().UTC().UnixNano()),
		JobID:        jobID,
		Status:       status,
		TriggeredBy:  "manual",
		ParamsJson:   "{}",
		QueuedAt:     queuedAt.UTC().Format(time.RFC3339Nano),
		StartedAt:    formatRetentionTime(startedAt),
		CompletedAt:  formatRetentionTime(completedAt),
		ParentRunID:  0,
		RerunStepKey: "",
		ErrorMessage: "",
	})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	if _, err := queries.RunStepCreate(ctx, db.RunStepCreateParams{
		RunID:        run.ID,
		JobNodeID:    0,
		StepKey:      "step_one",
		Status:       status,
		Attempt:      1,
		StartedAt:    formatRetentionTime(startedAt),
		CompletedAt:  formatRetentionTime(completedAt),
		DurationMs:   1200,
		OutputJson:   "{}",
		ErrorMessage: "",
		LogExcerpt:   "",
	}); err != nil {
		t.Fatalf("create run step: %v", err)
	}

	eventPayload, _ := json.Marshal(map[string]any{"status": status})
	if _, err := queries.RunEventCreate(ctx, db.RunEventCreateParams{
		RunID:         run.ID,
		StepKey:       "step_one",
		EventType:     "retention_fixture",
		Level:         "info",
		Message:       "fixture event",
		EventDataJson: string(eventPayload),
	}); err != nil {
		t.Fatalf("create run event: %v", err)
	}

	return run.ID
}

func createRetentionQueueItem(t *testing.T, ctx context.Context, queries db.Store, queueID int64, itemKey, partitionKey, status string, completedAt time.Time) int64 {
	t.Helper()

	item, err := queries.QueueItemCreate(ctx, db.QueueItemCreateParams{
		QueueID:      queueID,
		QueueItemKey: itemKey,
		PartitionKey: partitionKey,
		Status:       status,
		ExternalKey:  itemKey,
		PayloadJson:  `{"partition_key":"` + partitionKey + `"}`,
		ErrorMessage: "",
		QueuedAt:     completedAt.Add(-3 * time.Minute).UTC().Format(time.RFC3339Nano),
		StartedAt:    completedAt.Add(-2 * time.Minute).UTC().Format(time.RFC3339Nano),
		CompletedAt:  completedAt.UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("create queue item: %v", err)
	}
	return item.ID
}

func linkRetentionQueueRun(t *testing.T, ctx context.Context, queries db.Store, queueItemID, jobID, runID int64, stepKey string) {
	t.Helper()

	if _, err := queries.QueueItemRunCreate(ctx, db.QueueItemRunCreateParams{
		QueueItemID: queueItemID,
		JobID:       jobID,
		RunID:       runID,
	}); err != nil {
		t.Fatalf("create queue item run: %v", err)
	}
	if _, err := queries.QueueItemStepMetadataUpsert(ctx, db.QueueItemStepMetadataUpsertParams{
		QueueItemID:  queueItemID,
		JobID:        jobID,
		RunID:        runID,
		StepKey:      stepKey,
		MetadataJson: `{"source":"retention"}`,
	}); err != nil {
		t.Fatalf("create queue item metadata: %v", err)
	}
}

func assertCount(t *testing.T, ctx context.Context, pool *sql.DB, query string, id int64, want int) {
	t.Helper()

	var got int
	if err := pool.QueryRowContext(ctx, query, id).Scan(&got); err != nil {
		t.Fatalf("query count %q: %v", query, err)
	}
	if got != want {
		t.Fatalf("expected count %d for %q and id %d, got %d", want, query, id, got)
	}
}

func formatRetentionTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func TestRunRetentionSevenDaysPurgesExactlyTheOldRows(t *testing.T) {
	t.Parallel()

	queries, pool, err := db.NewTest()
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	t.Cleanup(func() {
		_ = pool.Close()
	})

	assertRetentionSevenDayPurge(t, queries, pool)
}

func TestRunRetentionSevenDaysPurgesExactlyTheOldRowsPostgres(t *testing.T) {
	dsn := os.Getenv(db.TestPostgresDSNEnv)
	if dsn == "" {
		t.Skipf("%s is not set", db.TestPostgresDSNEnv)
	}

	queries, pool, cleanup, err := db.NewTestPostgres(dsn)
	if err != nil {
		t.Fatalf("open postgres test db: %v", err)
	}
	t.Cleanup(cleanup)

	assertRetentionSevenDayPurge(t, queries, pool)
}

// assertRetentionSevenDayPurge seeds rows on both sides of a 7 day retention
// window and asserts that one purge removes exactly the old ones from runs,
// run steps, run events, and scheduler schedule-run records.
func assertRetentionSevenDayPurge(t *testing.T, queries db.Store, pool *sql.DB) {
	t.Helper()

	ctx := context.Background()
	now := time.Date(2026, time.March, 25, 12, 0, 0, 0, time.UTC)
	days := func(n int) time.Time { return now.AddDate(0, 0, -n) }

	jobID := createRetentionJob(t, ctx, queries, "retention_seven_day_job")

	// Old terminal runs: every one of these must be purged.
	oldSuccess := createRetentionRun(t, ctx, queries, jobID, "success", days(8))
	oldFailed := createRetentionRun(t, ctx, queries, jobID, "failed", days(10))
	oldCanceled := createRetentionRun(t, ctx, queries, jobID, "canceled", days(30))
	justOutside := createRetentionRun(t, ctx, queries, jobID, "success", days(7).Add(-time.Minute))

	// Runs inside the window, and runs that never finished: all must remain.
	justInside := createRetentionRun(t, ctx, queries, jobID, "success", days(7).Add(time.Minute))
	recentFailed := createRetentionRun(t, ctx, queries, jobID, "failed", days(1))
	oldRunning := createRetentionRunWithState(t, ctx, queries, jobID, "running", days(20), time.Time{})
	oldQueued := createRetentionRunWithState(t, ctx, queries, jobID, "queued", time.Time{}, time.Time{})

	purgedRuns := []int64{oldSuccess, oldFailed, oldCanceled, justOutside}
	keptRuns := []int64{justInside, recentFailed, oldRunning, oldQueued}

	runKey := func(runID int64) string {
		t.Helper()
		row, err := queries.RunGetByID(ctx, runID)
		if err != nil {
			t.Fatalf("load run %d: %v", runID, err)
		}
		return row.RunKey
	}
	claim := func(scheduledFor time.Time, key string) int64 {
		t.Helper()
		rows, err := queries.SchedulerScheduleRunsCreateIfAbsent(ctx, db.SchedulerScheduleRunsCreateIfAbsentParams{
			JobKey:       "retention_seven_day_job",
			ScheduleKey:  "every_minute",
			ScheduledFor: scheduledFor.UTC().Format(time.RFC3339Nano),
			RunKey:       key,
			TriggeredBy:  "scheduler:every_minute",
		})
		if err != nil {
			t.Fatalf("create schedule run: %v", err)
		}
		if len(rows) != 1 {
			t.Fatalf("expected one schedule run claim, got %d", len(rows))
		}
		return rows[0].ID
	}

	purgedClaims := []int64{
		claim(days(8), runKey(oldSuccess)),
		claim(days(10), runKey(oldFailed)),
		claim(days(7).Add(-time.Minute), runKey(justOutside)),
		// A claim whose run was never recorded.
		claim(days(9), ""),
	}
	keptClaims := []int64{
		claim(days(7).Add(time.Minute), runKey(justInside)),
		claim(days(1), runKey(recentFailed)),
		claim(days(2), ""),
		// Old claim, but its run is still running and is kept.
		claim(days(20), runKey(oldRunning)),
	}

	retention := NewRunRetention(queries, pool, RunRetentionOptions{RunDays: 7, BatchSize: 3})
	retention.nowFn = func() time.Time { return now }

	result, err := retention.RunOnce(ctx)
	if err != nil {
		t.Fatalf("run retention once: %v", err)
	}
	if result.DeletedRuns != int64(len(purgedRuns)) {
		t.Fatalf("expected %d deleted runs, got %d", len(purgedRuns), result.DeletedRuns)
	}
	if result.DeletedScheduleRuns != int64(len(purgedClaims)) {
		t.Fatalf("expected %d deleted schedule runs, got %d", len(purgedClaims), result.DeletedScheduleRuns)
	}

	assertRetentionIDs(t, ctx, pool, "runs", "SELECT id FROM runs ORDER BY id", keptRuns)
	assertRetentionIDs(t, ctx, pool, "run_steps", "SELECT DISTINCT run_id FROM run_steps ORDER BY run_id", keptRuns)
	assertRetentionIDs(t, ctx, pool, "run_events", "SELECT DISTINCT run_id FROM run_events ORDER BY run_id", keptRuns)
	assertRetentionIDs(t, ctx, pool, "scheduler_schedule_runs", "SELECT id FROM scheduler_schedule_runs ORDER BY id", keptClaims)

	for _, runID := range keptRuns {
		row, err := queries.RunGetByID(ctx, runID)
		if err != nil {
			t.Fatalf("expected run %d to remain: %v", runID, err)
		}
		if runID == oldRunning && row.Status != "running" {
			t.Fatalf("expected running run to be untouched, got status %q", row.Status)
		}
	}

	// A second pass has nothing left to do.
	second, err := retention.RunOnce(ctx)
	if err != nil {
		t.Fatalf("run retention twice: %v", err)
	}
	if second.DeletedRuns != 0 || second.DeletedQueueItems != 0 || second.DeletedScheduleRuns != 0 {
		t.Fatalf("expected second purge to delete nothing, got %+v", second)
	}
}

func assertRetentionIDs(t *testing.T, ctx context.Context, pool *sql.DB, table string, query string, want []int64) {
	t.Helper()

	rows, err := pool.QueryContext(ctx, query)
	if err != nil {
		t.Fatalf("query %s: %v", table, err)
	}
	defer rows.Close()

	got := make([]int64, 0, len(want))
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan %s: %v", table, err)
		}
		got = append(got, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows %s: %v", table, err)
	}

	sorted := append([]int64(nil), want...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	if fmt.Sprint(got) != fmt.Sprint(sorted) {
		t.Fatalf("%s: remaining ids = %v, want %v", table, got, sorted)
	}
}
