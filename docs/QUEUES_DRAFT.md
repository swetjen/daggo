# Queues Draft

Draft for review. This document captures the current queue direction discussed for DAGGO as of March 15, 2026.

## Summary

- `Queue` becomes a first-class DAGGO concept alongside jobs, runs, and schedules.
- The user owns queue storage and ingress mechanics.
- DAGGO owns orchestration after items are loaded from that user-owned queue.
- Queue-attached jobs are explicit and run as unordered fan-out peers.
- DAGGO does not expose a direct public enqueue API for bypassing queue semantics.
- Queue items do not auto-retry. Users can rerun failed jobs manually, and users enforce retries inside their ops if they want them.
- Virtuous is the canonical API layer for queue HTTP ingress when DAGGO exposes or mounts underlying API behavior.

## Locked Product Rules

- A queue may attach one or more jobs.
- For each accepted queue item, DAGGO creates one run per attached job.
- Attached jobs are unordered peers. Cross-job dependency ordering is out of scope at the queue layer.
- Repeated partition keys are allowed. Each arrival creates a new queue item event.
- Queue status is derived from linked runs.
- Queue list UI is item/event-centric.
- A secondary partition-centric UI view should exist, but it should be derived from queue items rather than replacing the main event list.
- Asset metadata is collected only from successful steps.
- Failed steps do not emit metadata.
- Failed runs may still show partial metadata from steps that completed successfully before the failure.
- Asset metadata uses `map[string]any`.
- Asset metadata is aggregated by `job_key -> step_key -> metadata`.
- Queue routes do not impose a DAGGO-owned response envelope. The user handles response particulars in their Virtuous handler.

## Public API Draft

This is the current recommended shape for the first queue API. It keeps job attachment explicit and keeps queue storage user-owned.

```go
package daggo

type QueueLoaderFunc[T any] func(ctx context.Context, emit func(LoadedQueueItem[T]) error) error

type LoadedQueueItem[T any] struct {
	Value       T
	ExternalKey string
	QueuedAt    time.Time
}

type QueueLoadMode string

const (
	QueueLoadModePoll   QueueLoadMode = "poll"
	QueueLoadModeStream QueueLoadMode = "stream"
)

type QueueLoaderOptions struct {
	Mode      QueueLoadMode
	PollEvery time.Duration
}

type QueueBuilder[T any] struct { /* draft */ }

func NewQueue[T any](key string) *QueueBuilder[T]

func (b *QueueBuilder[T]) WithDisplayName(name string) *QueueBuilder[T]
func (b *QueueBuilder[T]) WithDescription(description string) *QueueBuilder[T]
func (b *QueueBuilder[T]) WithRoute(path string, handler http.Handler) *QueueBuilder[T]
func (b *QueueBuilder[T]) WithLoader(loader QueueLoaderFunc[T], opts QueueLoaderOptions) *QueueBuilder[T]
func (b *QueueBuilder[T]) WithPartitionKey(fn func(T) (string, error)) *QueueBuilder[T]
func (b *QueueBuilder[T]) AddJobs(jobs ...dag.JobDefinition) *QueueBuilder[T]
func (b *QueueBuilder[T]) MustBuild() QueueDefinition
func (b *QueueBuilder[T]) Build() (QueueDefinition, error)
```

Notes:

- `WithLoader(...)` is the canonical queue ingestion contract. It is how DAGGO gets items from the user-owned queue.
- `WithRoute(...)` is optional. When present, DAGGO mounts the user-provided handler, typically written with Virtuous.
- The route should queue work into the user-owned backing store. It should not bypass queue semantics and invoke DAGGO jobs directly.
- `QueueLoadModePoll` means DAGGO calls the loader on a ticker and the loader drains available work.
- `QueueLoadModeStream` means DAGGO starts the loader once and the loader may block and emit items over time.
- `AddJobs(...)` remains explicit. The loader does not launch jobs directly.

Example:

```go
ordersQueue := daggo.NewQueue[OrderEnvelope]("orders_ingest").
	WithRoute("/queues/orders", virtuousHTTPHandler).
	WithLoader(loadOrders, daggo.QueueLoaderOptions{
		Mode:      daggo.QueueLoadModePoll,
		PollEvery: 2 * time.Second,
	}).
	WithPartitionKey(func(item OrderEnvelope) (string, error) {
		return item.OrderID, nil
	}).
	AddJobs(importJob, auditJob).
	MustBuild()
```

## Loader Contract Draft

The loader is the queue-to-DAGGO boundary.

Responsibilities:

- Read items from the user-owned queue source.
- Emit normalized items into DAGGO.
- Own source-specific ack/removal behavior.

DAGGO responsibilities:

- Persist the queue item row.
- Create linked run rows for attached jobs.
- Track queue item status from linked runs.
- Persist step metadata fragments as runs progress.

Recommended loader semantics:

- The loader should emit only normalized items that DAGGO can persist and fan out.
- The loader should return an error only for loader/runtime failure, not for ordinary per-item business failures.
- In poll mode, the loader should drain what is currently available and return.
- In stream mode, the loader may block until context cancellation and emit items over time.

Open review points in this draft:

- Whether `LoadedQueueItem[T]` also needs source metadata such as headers or queue-specific tags.
- Whether `ExternalKey` should be promoted to a first-class idempotency field in v1 or remain informational only.

## Queue Item Record Draft

Recommended normalized core model:

```go
type QueueItemRecord struct {
	ID            int64
	QueueKey      string
	PartitionKey  string
	Status        string
	ExternalKey   string
	PayloadJSON   string
	ErrorMessage  string
	QueuedAt      string
	StartedAt     string
	CompletedAt   string
	CreatedAt     string
	UpdatedAt     string
}
```

Recommended status rules:

- `queued`: all linked runs are still queued or not yet started.
- `running`: at least one linked run is queued/running and at least one run has been created.
- `complete`: all linked runs reached success.
- `failed`: all linked runs reached terminal state and at least one failed or was canceled.

The UI can render `queued` as "Queued / Not Started" while the backend keeps the status set small.

## Persistence Draft

Recommended v1 schema direction:

### `queues`

Registry-synced queue definitions for UI and linkage.

Suggested fields:

- `id`
- `queue_key`
- `display_name`
- `description`
- `route_path`
- `load_mode`
- `load_poll_every_seconds`
- timestamps

### `queue_jobs`

Explicit mapping between a queue and its attached jobs.

Suggested fields:

- `id`
- `queue_id`
- `job_id`
- `sort_index`
- timestamps

### `queue_items`

One row per accepted queue item event.

Suggested fields:

- `id`
- `queue_id`
- `queue_item_key`
- `partition_key`
- `status`
- `external_key`
- `payload_json`
- `error_message`
- `queued_at`
- `started_at`
- `completed_at`
- timestamps

### `queue_item_runs`

Links queue items to the per-job runs DAGGO launches.

Suggested fields:

- `id`
- `queue_item_id`
- `job_id`
- `run_id`
- timestamps

### `queue_item_step_metadata`

Stores metadata fragments from successful steps without requiring concurrent JSON merging on the parent item row.

Suggested fields:

- `id`
- `queue_item_id`
- `job_id`
- `run_id`
- `step_key`
- `metadata_json`
- timestamps

Recommended v1 rule:

- Do not add a dedicated `queue_partitions` table yet.
- Build the partition-centric UI from `queue_items`, grouped by `partition_key`, until we prove we need a materialized partition state table.

## Runtime Notes

- Queue definitions should sync at startup in the same spirit as job registry sync.
- A queue item should be the durable object DAGGO tracks.
- Runs launched from queue items should carry queue linkage in a way the executor can resolve during step completion.
- Successful step completion should upsert one metadata fragment per `(queue_item_id, job_id, run_id, step_key)`.
- Queue detail handlers should assemble aggregated metadata as:

```json
{
  "job_a": {
    "step_one": { "foo": "bar" },
    "step_two": { "count": 3 }
  },
  "job_b": {
    "enrich": { "lookup_id": "abc123" }
  }
}
```

## Open Questions

- Whether `WithLoader(...)` should be required for every queue or whether a future non-HTTP ingest model should be able to provide an equivalent runtime source.
- Whether queue item payload persistence should be required JSON only, or whether binary payload support should eventually exist.
- Whether queue item status should remain purely derived or also be cached onto `queue_items` for faster list queries.
