package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/swetjen/daggo"
	"github.com/swetjen/daggo/dag"
)

type ImportEnvelope struct {
	CustomerID string `json:"customer_id"`
	BatchID    string `json:"batch_id"`
}

type ImportResult struct {
	CustomerID string `json:"customer_id"`
	BatchID    string `json:"batch_id"`
	ExternalID string `json:"external_id"`
}

func (r ImportResult) DaggoMetadata() map[string]any {
	return map[string]any{
		"customer_id": r.CustomerID,
		"batch_id":    r.BatchID,
		"external_id": r.ExternalID,
	}
}

type Ops struct{}

func (o *Ops) ImportCustomer(ctx context.Context, _ dag.NoInput) (ImportResult, error) {
	payload, ok, err := daggo.QueuePayloadFromContext[ImportEnvelope](ctx)
	if err != nil {
		return ImportResult{}, err
	}
	if !ok {
		return ImportResult{}, fmt.Errorf("queue payload is unavailable")
	}
	return ImportResult{
		CustomerID: payload.CustomerID,
		BatchID:    payload.BatchID,
		ExternalID: "crm:" + payload.CustomerID,
	}, nil
}

func customerImportJob(ops *Ops) dag.JobDefinition {
	importCustomer := dag.Op[dag.NoInput, ImportResult]("import_customer", ops.ImportCustomer)
	return dag.NewJob("customer_import").
		Add(importCustomer).
		MustBuild()
}

func main() {
	cfg := daggo.DefaultConfig()
	cfg.Admin.Port = "8080"
	cfg.Database.SQLite.Path = "/tmp/daggo-queue-example.sqlite"

	incoming := make(chan daggo.LoadedQueueItem[ImportEnvelope], 1)
	incoming <- daggo.LoadedQueueItem[ImportEnvelope]{
		Value: ImportEnvelope{
			CustomerID: "customer-123",
			BatchID:    "batch-001",
		},
		ExternalKey: "webhook:1",
		QueuedAt:    time.Now(),
	}
	close(incoming)

	loadFromChannel := func(ctx context.Context, emit func(daggo.LoadedQueueItem[ImportEnvelope]) error) error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case item, ok := <-incoming:
				if !ok {
					return nil
				}
				if err := emit(item); err != nil {
					return err
				}
			}
		}
	}

	importQueue := daggo.NewQueue[ImportEnvelope]("customer_imports").
		WithDescription("Example queue using a channel-backed loader").
		WithLoader(loadFromChannel, daggo.QueueLoaderOptions{
			Mode: daggo.QueueLoadModeStream,
		}).
		WithPartitionKey(func(item ImportEnvelope) (string, error) {
			if item.CustomerID == "" {
				return "", fmt.Errorf("customer_id is required")
			}
			return item.CustomerID, nil
		}).
		AddJobs(customerImportJob(&Ops{})).
		MustBuild()

	if err := daggo.RunDefinitions(context.Background(), cfg, importQueue); err != nil {
		log.Fatal(err)
	}
}
