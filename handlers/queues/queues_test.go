package queues

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/swetjen/daggo/dag"
	"github.com/swetjen/daggo/db"
	"github.com/swetjen/daggo/deps"
	"github.com/swetjen/daggo/queue"
	"github.com/swetjen/virtuous/rpc"
)

type queueHandlerPayload struct {
	CustomerID string `json:"customer_id"`
}

type queueHandlerOutput struct {
	CustomerID string `json:"customer_id"`
}

func TestQueueHandlers_ListPartitionAndDetail(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	queries, pool, err := db.NewTest()
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	t.Cleanup(func() {
		_ = pool.Close()
	})

	job := dag.NewJob("queue_handler_job").
		Add(dag.Op[dag.NoInput, queueHandlerOutput]("publish_metadata", func(_ context.Context, _ dag.NoInput) (queueHandlerOutput, error) {
			return queueHandlerOutput{CustomerID: "cust_123"}, nil
		})).
		MustBuild()

	jobRegistry := dag.NewRegistry()
	if err := jobRegistry.Register(job); err != nil {
		t.Fatalf("register job: %v", err)
	}
	if err := jobRegistry.SyncToDB(ctx, queries, pool); err != nil {
		t.Fatalf("sync jobs: %v", err)
	}

	queueRegistry := queue.NewRegistry()
	definition := queue.New[queueHandlerPayload]("customer_queue").
		WithDisplayName("Customer Queue").
		WithDescription("Queue for customer work items").
		WithLoader(func(ctx context.Context, emit func(queue.LoadedItem[queueHandlerPayload]) error) error { return nil }, queue.LoaderOptions{}).
		WithPartitionKey(func(item queueHandlerPayload) (string, error) { return item.CustomerID, nil }).
		AddJobs(job).
		MustBuild()
	if err := queueRegistry.Register(definition); err != nil {
		t.Fatalf("register queue: %v", err)
	}
	if err := queueRegistry.SyncToDB(ctx, queries, pool); err != nil {
		t.Fatalf("sync queues: %v", err)
	}

	queueRow, err := queries.QueueGetByKey(ctx, definition.Key)
	if err != nil {
		t.Fatalf("load queue row: %v", err)
	}
	jobRow, err := queries.JobGetByKey(ctx, job.Key)
	if err != nil {
		t.Fatalf("load job row: %v", err)
	}

	base := time.Date(2026, 3, 15, 2, 0, 0, 0, time.UTC)
	ts := func(minutes int) string {
		return base.Add(time.Duration(minutes) * time.Minute).Format(time.RFC3339Nano)
	}

	item1, err := queries.QueueItemCreate(ctx, db.QueueItemCreateParams{
		QueueID:      queueRow.ID,
		QueueItemKey: "qit_old",
		PartitionKey: "cust_123",
		Status:       "complete",
		ExternalKey:  "ext_old",
		PayloadJson:  `{"customer_id":"cust_123"}`,
		ErrorMessage: "",
		QueuedAt:     ts(0),
		StartedAt:    ts(1),
		CompletedAt:  ts(2),
	})
	if err != nil {
		t.Fatalf("create first queue item: %v", err)
	}

	item2, err := queries.QueueItemCreate(ctx, db.QueueItemCreateParams{
		QueueID:      queueRow.ID,
		QueueItemKey: "qit_latest",
		PartitionKey: "cust_123",
		Status:       "failed",
		ExternalKey:  "ext_latest",
		PayloadJson:  `{"customer_id":"cust_123"}`,
		ErrorMessage: "sync exploded",
		QueuedAt:     ts(10),
		StartedAt:    ts(11),
		CompletedAt:  ts(13),
	})
	if err != nil {
		t.Fatalf("create second queue item: %v", err)
	}

	run, err := queries.RunCreate(ctx, db.RunCreateParams{
		RunKey:       fmt.Sprintf("queue_handler_run_%d", time.Now().UnixNano()),
		JobID:        jobRow.ID,
		Status:       "failed",
		TriggeredBy:  "queue:customer_queue",
		ParamsJson:   "{}",
		QueuedAt:     ts(10),
		StartedAt:    ts(11),
		CompletedAt:  ts(13),
		ParentRunID:  0,
		RerunStepKey: "",
		ErrorMessage: "sync exploded",
	})
	if err != nil {
		t.Fatalf("create linked run: %v", err)
	}
	if _, err := queries.QueueItemRunCreate(ctx, db.QueueItemRunCreateParams{
		QueueItemID: item2.ID,
		JobID:       jobRow.ID,
		RunID:       run.ID,
	}); err != nil {
		t.Fatalf("create queue item run: %v", err)
	}
	if _, err := queries.QueueItemStepMetadataUpsert(ctx, db.QueueItemStepMetadataUpsertParams{
		QueueItemID:  item2.ID,
		JobID:        jobRow.ID,
		RunID:        run.ID,
		StepKey:      "publish_metadata",
		MetadataJson: `{"customer_id":"cust_123","external_id":"crm:cust_123"}`,
	}); err != nil {
		t.Fatalf("create metadata row: %v", err)
	}

	handler := New(&deps.Deps{
		DB:       queries,
		Pool:     pool,
		Registry: jobRegistry,
		Queues:   queueRegistry,
	})

	queuesResp, status := handler.QueuesGetMany(ctx, QueuesGetManyRequest{Limit: 50, Offset: 0})
	if status != rpc.StatusOK {
		t.Fatalf("expected ok status, got %d", status)
	}
	if queuesResp.Total != 1 || len(queuesResp.Data) != 1 {
		t.Fatalf("expected one queue summary, got total=%d len=%d", queuesResp.Total, len(queuesResp.Data))
	}
	summary := queuesResp.Data[0]
	if summary.ItemCount != 2 {
		t.Fatalf("expected item count 2, got %d", summary.ItemCount)
	}
	if summary.PartitionCount != 1 {
		t.Fatalf("expected partition count 1, got %d", summary.PartitionCount)
	}
	if summary.CompleteCount != 1 || summary.FailedCount != 1 {
		t.Fatalf("expected complete=1 failed=1, got complete=%d failed=%d", summary.CompleteCount, summary.FailedCount)
	}
	if len(summary.Jobs) != 1 || summary.Jobs[0].JobKey != job.Key {
		t.Fatalf("expected queue summary to include attached job, got %+v", summary.Jobs)
	}

	partitionsResp, status := handler.QueuePartitionsGetMany(ctx, QueuePartitionsGetManyRequest{
		QueueKey: definition.Key,
		Limit:    50,
		Offset:   0,
	})
	if status != rpc.StatusOK {
		t.Fatalf("expected partitions ok status, got %d", status)
	}
	if partitionsResp.Total != 1 || len(partitionsResp.Data) != 1 {
		t.Fatalf("expected one partition summary, got total=%d len=%d", partitionsResp.Total, len(partitionsResp.Data))
	}
	partition := partitionsResp.Data[0]
	if partition.LatestQueueItemID != item2.ID {
		t.Fatalf("expected latest partition row to point at newest queue item %d, got %d", item2.ID, partition.LatestQueueItemID)
	}
	if partition.QueueItemKey != item2.QueueItemKey || partition.Status != "failed" {
		t.Fatalf("expected latest failed partition summary, got %+v", partition)
	}

	itemResp, status := handler.QueueItemByID(ctx, QueueItemByIDRequest{ID: item2.ID})
	if status != rpc.StatusOK {
		t.Fatalf("expected item detail ok status, got %d", status)
	}
	if itemResp.Item.ID != item2.ID || itemResp.Item.QueueKey != definition.Key {
		t.Fatalf("unexpected queue item detail %+v", itemResp.Item)
	}
	if len(itemResp.Runs) != 1 || itemResp.Runs[0].RunKey != run.RunKey {
		t.Fatalf("expected one linked run, got %+v", itemResp.Runs)
	}
	jobMetadata, ok := itemResp.Item.Metadata[job.Key]
	if !ok {
		t.Fatalf("expected metadata under job key %q, got %+v", job.Key, itemResp.Item.Metadata)
	}
	stepMetadata, ok := jobMetadata["publish_metadata"].(map[string]any)
	if !ok {
		t.Fatalf("expected step metadata map, got %#v", jobMetadata["publish_metadata"])
	}
	if stepMetadata["external_id"] != "crm:cust_123" {
		t.Fatalf("expected external_id metadata, got %#v", stepMetadata["external_id"])
	}

	itemsResp, status := handler.QueueItemsGetMany(ctx, QueueItemsGetManyRequest{})
	if status != rpc.StatusInvalid {
		t.Fatalf("expected invalid status for empty queue key, got %d", status)
	}
	if itemsResp.Error == "" {
		t.Fatalf("expected validation error for missing queue key")
	}

	rawPayload := make(map[string]any)
	if err := json.Unmarshal([]byte(itemResp.Item.PayloadJSON), &rawPayload); err != nil {
		t.Fatalf("decode item payload json: %v", err)
	}
	if rawPayload["customer_id"] != "cust_123" {
		t.Fatalf("expected payload customer_id cust_123, got %#v", rawPayload["customer_id"])
	}

	_ = item1
}
