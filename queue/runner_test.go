package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/swetjen/daggo/dag"
	"github.com/swetjen/daggo/db"
)

type queueRunnerPayload struct {
	OrderID string `json:"order_id"`
}

type queueRunnerOutput struct {
	OrderID string `json:"order_id"`
}

func (o queueRunnerOutput) DaggoMetadata() map[string]any {
	return map[string]any{
		"order_id": o.OrderID,
	}
}

type queueFailingOutput struct {
	OrderID string `json:"order_id"`
}

func (o queueFailingOutput) DaggoMetadata() map[string]any {
	return map[string]any{
		"order_id": o.OrderID,
	}
}

type staticDeployDrainer struct {
	draining bool
}

func (d staticDeployDrainer) IsDraining() bool {
	return d.draining
}

func TestRunnerRunLoaderOnce_CreatesQueueItemsRunsAndMetadata(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	queries, pool, err := db.NewTest()
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	t.Cleanup(func() {
		_ = pool.Close()
	})

	job := dag.NewJob("queue_runner_job").
		Add(dag.Op[dag.NoInput, queueRunnerOutput]("emit_metadata", func(ctx context.Context, _ dag.NoInput) (queueRunnerOutput, error) {
			meta, ok := dag.RunQueueMetaFromContext(ctx)
			if !ok {
				return queueRunnerOutput{}, fmt.Errorf("expected queue meta in context")
			}
			payloadMap, ok := meta.Payload.(map[string]any)
			if !ok {
				return queueRunnerOutput{}, fmt.Errorf("expected payload map in context, got %T", meta.Payload)
			}
			orderID, _ := payloadMap["order_id"].(string)
			return queueRunnerOutput{OrderID: orderID}, nil
		})).
		MustBuild()

	registry := dag.NewRegistry()
	if err := registry.Register(job); err != nil {
		t.Fatalf("register job: %v", err)
	}
	if err := registry.SyncToDB(ctx, queries, pool); err != nil {
		t.Fatalf("sync jobs: %v", err)
	}

	queueRegistry := NewRegistry()
	definition := New[queueRunnerPayload]("orders_queue").
		WithLoader(func(ctx context.Context, emit func(LoadedItem[queueRunnerPayload]) error) error {
			return emit(LoadedItem[queueRunnerPayload]{
				Value:       queueRunnerPayload{OrderID: "ord_123"},
				ExternalKey: "ext_123",
				QueuedAt:    time.Now().UTC(),
			})
		}, LoaderOptions{}).
		WithPartitionKey(func(item queueRunnerPayload) (string, error) { return item.OrderID, nil }).
		AddJobs(job).
		MustBuild()
	if err := queueRegistry.Register(definition); err != nil {
		t.Fatalf("register queue: %v", err)
	}
	if err := queueRegistry.SyncToDB(ctx, queries, pool); err != nil {
		t.Fatalf("sync queues: %v", err)
	}

	executor := dag.NewExecutor(queries, pool, registry, 8)
	executor.SetExecutionMode(dag.ExecutionModeInProcess)
	runner := NewRunner(queries, pool, queueRegistry, executor, nil)

	runner.runLoaderOnce(ctx, definition)
	deadline := time.Now().Add(2 * time.Second)
	for !executor.IsIdle() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !executor.IsIdle() {
		t.Fatalf("executor did not go idle")
	}

	queueRow, err := queries.QueueGetByKey(ctx, definition.Key)
	if err != nil {
		t.Fatalf("load queue: %v", err)
	}
	items, err := queries.QueueItemGetManyByQueueID(ctx, db.QueueItemGetManyByQueueIDParams{
		QueueID: queueRow.ID,
		Limit:   20,
		Offset:  0,
	})
	if err != nil {
		t.Fatalf("load queue items: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected one queue item, got %d", len(items))
	}
	if items[0].Status != "complete" {
		t.Fatalf("expected queue item complete, got %q", items[0].Status)
	}
	if items[0].PartitionKey != "ord_123" {
		t.Fatalf("expected partition key ord_123, got %q", items[0].PartitionKey)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(items[0].PayloadJson), &payload); err != nil {
		t.Fatalf("decode queue item payload: %v", err)
	}
	if payload["order_id"] != "ord_123" {
		t.Fatalf("expected payload order_id ord_123, got %#v", payload["order_id"])
	}

	runRows, err := queries.QueueItemRunGetManyByQueueItemIDJoinedRuns(ctx, items[0].ID)
	if err != nil {
		t.Fatalf("load queue item runs: %v", err)
	}
	if len(runRows) != 1 {
		t.Fatalf("expected one linked run, got %d", len(runRows))
	}
	if runRows[0].RunStatus != "success" {
		t.Fatalf("expected linked run success, got %q", runRows[0].RunStatus)
	}

	metadataRows, err := queries.QueueItemStepMetadataGetManyByQueueItemID(ctx, items[0].ID)
	if err != nil {
		t.Fatalf("load queue metadata: %v", err)
	}
	if len(metadataRows) != 1 {
		t.Fatalf("expected one metadata row, got %d", len(metadataRows))
	}
	var metadata map[string]any
	if err := json.Unmarshal([]byte(metadataRows[0].MetadataJson), &metadata); err != nil {
		t.Fatalf("decode metadata: %v", err)
	}
	if metadata["order_id"] != "ord_123" {
		t.Fatalf("expected metadata order_id ord_123, got %#v", metadata["order_id"])
	}
}

func TestRunnerRunLoaderOnce_MultiJobFanoutFailureMarksQueueItemFailed(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	queries, pool, err := db.NewTest()
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	t.Cleanup(func() {
		_ = pool.Close()
	})

	successJob := dag.NewJob("queue_fanout_success_job").
		Add(dag.Op[dag.NoInput, queueRunnerOutput]("publish_metadata", func(ctx context.Context, _ dag.NoInput) (queueRunnerOutput, error) {
			meta, ok := dag.RunQueueMetaFromContext(ctx)
			if !ok {
				return queueRunnerOutput{}, fmt.Errorf("expected queue meta in context")
			}
			payloadMap, ok := meta.Payload.(map[string]any)
			if !ok {
				return queueRunnerOutput{}, fmt.Errorf("expected payload map in context, got %T", meta.Payload)
			}
			orderID, _ := payloadMap["order_id"].(string)
			return queueRunnerOutput{OrderID: orderID}, nil
		})).
		MustBuild()

	failingJob := dag.NewJob("queue_fanout_failed_job").
		Add(dag.Op[dag.NoInput, queueFailingOutput]("always_fail", func(ctx context.Context, _ dag.NoInput) (queueFailingOutput, error) {
			meta, ok := dag.RunQueueMetaFromContext(ctx)
			if !ok {
				return queueFailingOutput{}, fmt.Errorf("expected queue meta in context")
			}
			payloadMap, ok := meta.Payload.(map[string]any)
			if !ok {
				return queueFailingOutput{}, fmt.Errorf("expected payload map in context, got %T", meta.Payload)
			}
			orderID, _ := payloadMap["order_id"].(string)
			return queueFailingOutput{OrderID: orderID}, fmt.Errorf("job failed for %s", orderID)
		})).
		MustBuild()

	registry := dag.NewRegistry()
	for _, job := range []dag.JobDefinition{successJob, failingJob} {
		if err := registry.Register(job); err != nil {
			t.Fatalf("register job %s: %v", job.Key, err)
		}
	}
	if err := registry.SyncToDB(ctx, queries, pool); err != nil {
		t.Fatalf("sync jobs: %v", err)
	}

	queueRegistry := NewRegistry()
	definition := New[queueRunnerPayload]("orders_fanout_queue").
		WithLoader(func(ctx context.Context, emit func(LoadedItem[queueRunnerPayload]) error) error {
			return emit(LoadedItem[queueRunnerPayload]{
				Value:       queueRunnerPayload{OrderID: "ord_456"},
				ExternalKey: "ext_456",
				QueuedAt:    time.Now().UTC(),
			})
		}, LoaderOptions{}).
		WithPartitionKey(func(item queueRunnerPayload) (string, error) { return item.OrderID, nil }).
		AddJobs(successJob, failingJob).
		MustBuild()
	if err := queueRegistry.Register(definition); err != nil {
		t.Fatalf("register queue: %v", err)
	}
	if err := queueRegistry.SyncToDB(ctx, queries, pool); err != nil {
		t.Fatalf("sync queues: %v", err)
	}

	executor := dag.NewExecutor(queries, pool, registry, 8)
	executor.SetExecutionMode(dag.ExecutionModeInProcess)
	runner := NewRunner(queries, pool, queueRegistry, executor, nil)

	runner.runLoaderOnce(ctx, definition)
	waitForExecutorIdle(t, executor)

	queueRow, err := queries.QueueGetByKey(ctx, definition.Key)
	if err != nil {
		t.Fatalf("load queue: %v", err)
	}
	items, err := queries.QueueItemGetManyByQueueID(ctx, db.QueueItemGetManyByQueueIDParams{
		QueueID: queueRow.ID,
		Limit:   20,
		Offset:  0,
	})
	if err != nil {
		t.Fatalf("load queue items: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected one queue item, got %d", len(items))
	}
	if items[0].Status != "failed" {
		t.Fatalf("expected queue item failed, got %q", items[0].Status)
	}
	if !strings.Contains(items[0].ErrorMessage, "job failed for ord_456") {
		t.Fatalf("expected queue item error to include failed run, got %q", items[0].ErrorMessage)
	}

	runRows, err := queries.QueueItemRunGetManyByQueueItemIDJoinedRuns(ctx, items[0].ID)
	if err != nil {
		t.Fatalf("load queue item runs: %v", err)
	}
	if len(runRows) != 2 {
		t.Fatalf("expected two linked runs, got %d", len(runRows))
	}
	runStatusByJob := make(map[string]string, len(runRows))
	for _, row := range runRows {
		runStatusByJob[row.JobKey] = row.RunStatus
	}
	if runStatusByJob[successJob.Key] != "success" {
		t.Fatalf("expected success job run success, got %q", runStatusByJob[successJob.Key])
	}
	if runStatusByJob[failingJob.Key] != "failed" {
		t.Fatalf("expected failing job run failed, got %q", runStatusByJob[failingJob.Key])
	}

	metadataRows, err := queries.QueueItemStepMetadataGetManyByQueueItemID(ctx, items[0].ID)
	if err != nil {
		t.Fatalf("load queue metadata: %v", err)
	}
	if len(metadataRows) != 1 {
		t.Fatalf("expected only successful step metadata to persist, got %d rows", len(metadataRows))
	}
	if metadataRows[0].StepKey != "publish_metadata" {
		t.Fatalf("expected metadata from successful step, got %q", metadataRows[0].StepKey)
	}
}

func TestRunnerRunLoaderOnce_DeployDrainingSkipsWorkWithoutPersistingRows(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	queries, pool, err := db.NewTest()
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	t.Cleanup(func() {
		_ = pool.Close()
	})

	job := dag.NewJob("queue_drain_job").
		Add(dag.Op[dag.NoInput, queueRunnerOutput]("publish_metadata", func(_ context.Context, _ dag.NoInput) (queueRunnerOutput, error) {
			return queueRunnerOutput{OrderID: "ignored"}, nil
		})).
		MustBuild()

	registry := dag.NewRegistry()
	if err := registry.Register(job); err != nil {
		t.Fatalf("register job: %v", err)
	}
	if err := registry.SyncToDB(ctx, queries, pool); err != nil {
		t.Fatalf("sync jobs: %v", err)
	}

	queueRegistry := NewRegistry()
	definition := New[queueRunnerPayload]("orders_drain_queue").
		WithLoader(func(ctx context.Context, emit func(LoadedItem[queueRunnerPayload]) error) error {
			return emit(LoadedItem[queueRunnerPayload]{
				Value:       queueRunnerPayload{OrderID: "ord_blocked"},
				ExternalKey: "ext_blocked",
				QueuedAt:    time.Now().UTC(),
			})
		}, LoaderOptions{}).
		WithPartitionKey(func(item queueRunnerPayload) (string, error) { return item.OrderID, nil }).
		AddJobs(job).
		MustBuild()
	if err := queueRegistry.Register(definition); err != nil {
		t.Fatalf("register queue: %v", err)
	}
	if err := queueRegistry.SyncToDB(ctx, queries, pool); err != nil {
		t.Fatalf("sync queues: %v", err)
	}

	executor := dag.NewExecutor(queries, pool, registry, 8)
	executor.SetExecutionMode(dag.ExecutionModeInProcess)
	runner := NewRunner(queries, pool, queueRegistry, executor, staticDeployDrainer{draining: true})

	runner.runLoaderOnce(ctx, definition)

	queueRow, err := queries.QueueGetByKey(ctx, definition.Key)
	if err != nil {
		t.Fatalf("load queue: %v", err)
	}
	items, err := queries.QueueItemGetManyByQueueID(ctx, db.QueueItemGetManyByQueueIDParams{
		QueueID: queueRow.ID,
		Limit:   20,
		Offset:  0,
	})
	if err != nil {
		t.Fatalf("load queue items: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("expected no queue items while deploy draining, got %d", len(items))
	}
}

func waitForExecutorIdle(t *testing.T, executor *dag.Executor) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for !executor.IsIdle() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !executor.IsIdle() {
		t.Fatalf("executor did not go idle")
	}
}
