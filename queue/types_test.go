package queue

import (
	"context"
	"net/http"
	"testing"

	"github.com/swetjen/daggo/dag"
)

type queueValidationPayload struct {
	ID string `json:"id"`
}

func TestQueueBuilderBuild_ValidatesRequiredFields(t *testing.T) {
	t.Parallel()

	job := dag.NewJob("queue_validation_job").
		Add(dag.Op[dag.NoInput, queueValidationPayload]("source", func(context.Context, dag.NoInput) (queueValidationPayload, error) {
			return queueValidationPayload{ID: "ok"}, nil
		})).
		MustBuild()

	_, err := New[queueValidationPayload]("missing_loader").
		WithPartitionKey(func(item queueValidationPayload) (string, error) { return item.ID, nil }).
		AddJobs(job).
		Build()
	if err == nil || err.Error() != `queue "missing_loader" loader is required` {
		t.Fatalf("expected loader validation error, got %v", err)
	}

	_, err = New[queueValidationPayload]("reserved_route").
		WithRoute("/queues/incoming", http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})).
		WithLoader(func(ctx context.Context, emit func(LoadedItem[queueValidationPayload]) error) error { return nil }, LoaderOptions{}).
		WithPartitionKey(func(item queueValidationPayload) (string, error) { return item.ID, nil }).
		AddJobs(job).
		Build()
	if err == nil || err.Error() != `queue "reserved_route" route: route path "/queues/incoming" conflicts with DAGGO reserved routes` {
		t.Fatalf("expected reserved route validation error, got %v", err)
	}
}

func TestQueueBuilderBuild_ValidatesDuplicateJobsAndRoutePairing(t *testing.T) {
	t.Parallel()

	job := dag.NewJob("queue_duplicate_job").
		Add(dag.Op[dag.NoInput, queueValidationPayload]("source", func(context.Context, dag.NoInput) (queueValidationPayload, error) {
			return queueValidationPayload{ID: "ok"}, nil
		})).
		MustBuild()

	_, err := New[queueValidationPayload]("duplicate_jobs").
		WithLoader(func(ctx context.Context, emit func(LoadedItem[queueValidationPayload]) error) error { return nil }, LoaderOptions{}).
		WithPartitionKey(func(item queueValidationPayload) (string, error) { return item.ID, nil }).
		AddJobs(job, job).
		Build()
	if err == nil || err.Error() != `queue "duplicate_jobs" includes duplicate job "queue_duplicate_job"` {
		t.Fatalf("expected duplicate job validation error, got %v", err)
	}

	_, err = New[queueValidationPayload]("missing_route_handler").
		WithRoute("/incoming", nil).
		WithLoader(func(ctx context.Context, emit func(LoadedItem[queueValidationPayload]) error) error { return nil }, LoaderOptions{}).
		WithPartitionKey(func(item queueValidationPayload) (string, error) { return item.ID, nil }).
		AddJobs(job).
		Build()
	if err == nil || err.Error() != `queue "missing_route_handler" route: handler is required when route path is set` {
		t.Fatalf("expected handler-required validation error, got %v", err)
	}

	_, err = New[queueValidationPayload]("missing_route_path").
		WithRoute("", http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})).
		WithLoader(func(ctx context.Context, emit func(LoadedItem[queueValidationPayload]) error) error { return nil }, LoaderOptions{}).
		WithPartitionKey(func(item queueValidationPayload) (string, error) { return item.ID, nil }).
		AddJobs(job).
		Build()
	if err == nil || err.Error() != `queue "missing_route_path" route: route path is required when handler is set` {
		t.Fatalf("expected route-path-required validation error, got %v", err)
	}
}

func TestQueueBuilderBuild_NormalizesLoadModeAndPollInterval(t *testing.T) {
	t.Parallel()

	job := dag.NewJob("queue_mode_job").
		Add(dag.Op[dag.NoInput, queueValidationPayload]("source", func(context.Context, dag.NoInput) (queueValidationPayload, error) {
			return queueValidationPayload{ID: "ok"}, nil
		})).
		MustBuild()

	definition, err := New[queueValidationPayload]("normalized_mode").
		WithLoader(func(ctx context.Context, emit func(LoadedItem[queueValidationPayload]) error) error { return nil }, LoaderOptions{
			Mode:      LoadMode("invalid"),
			PollEvery: 0,
		}).
		WithPartitionKey(func(item queueValidationPayload) (string, error) { return item.ID, nil }).
		AddJobs(job).
		Build()
	if err != nil {
		t.Fatalf("build queue: %v", err)
	}
	if definition.LoadMode != LoadModePoll {
		t.Fatalf("expected invalid load mode to normalize to poll, got %q", definition.LoadMode)
	}
	if definition.LoadPollEvery <= 0 {
		t.Fatalf("expected poll interval default to be set, got %s", definition.LoadPollEvery)
	}
}
