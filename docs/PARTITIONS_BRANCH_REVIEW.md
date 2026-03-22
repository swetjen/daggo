# `feat/partitions` Review For Queues

This note evaluates the current `feat/partitions` branch against the queue design drafted in [QUEUES_DRAFT.md](./QUEUES_DRAFT.md).

## Recommendation

Do not merge `feat/partitions` as-is for the queue feature.

It contains useful ideas and some reusable building blocks, but its core model is centered on op-attached partition definitions and backfill launch flows. The queue feature needs queue-owned item events, loader-driven ingestion, and asset metadata aggregation. Those are materially different product shapes.

## Short Recommendations For The Partitions Team

- Keep partition runtime context and key-normalization helpers reusable and queue-compatible.
- Do not center the implementation on op-level `WithPartition(...)` declarations for queue work.
- Do not treat backfills or partition selection as the primary queue data model.
- Target a secondary partition view under top-level queues, not a jobs/backfills-first UI.
- Assume queue items are the primary unit of tracking; partitions summarize across those items.

## What Fits

### 1. Run partition context is reusable

The branch extends `dag.RunMeta` with `RunPartitionMeta` and provides `dag.RunPartitionMetaFromContext(ctx)`. That idea fits queue-linked runs well if DAGGO wants step code to read queue partition information from `context.Context`.

Why it fits:

- queue runs also need partition context
- it keeps runtime metadata out of step signatures
- it is additive to the existing run metadata model

Recommendation:

- Reuse the runtime-context approach
- Rename or reshape it only if queue semantics need a more queue-specific metadata envelope

### 2. Partition normalization utilities are useful

The branch includes solid normalization and selection helpers for partition keys and time/static partition domains.

Why it fits:

- queue partition views will still benefit from normalized partition keys
- later queue + partitions work may need shared helpers

Recommendation:

- Cherry-pick helpers, not the full op-partition DSL

## What Does Not Fit

### 1. Partitions are attached to ops, not queues

The branch makes partitions a property of `dag.Op(...).WithPartition(...)`.

Why it conflicts:

- queue partitions are item-level routing/identity, not op-declared materialization domains
- queue ingestion needs one partition key per queue item event
- the queue design does not want queue authors to retrofit partition declarations onto step definitions just to ingest work

### 2. Backfill-first data model is the wrong center of gravity

The branch adds:

- `partition_definitions`
- `partition_keys`
- `backfills`
- `backfill_partitions`
- `run_partition_targets`
- `run_system_tags`

Why it conflicts:

- queue needs `queue_items`, queue-to-run linkage, and per-step asset metadata
- backfill launch is job-centric and selection-centric
- queue ingestion is source-centric and event-centric

The branch solves "launch many partition-targeted runs" rather than "accept external work and track it through a queue item lifecycle."

### 3. UI placement is wrong for queues

The branch puts the feature under Jobs and Backfills.

Why it conflicts:

- queue is intended to be a top-level concept
- the primary view needs queue item events and queue state
- partition view is secondary, not the main product surface

### 4. No queue loader or user-owned storage model

The branch assumes DAGGO already owns the launch surface.

Why it conflicts:

- queue design explicitly leaves storage and ingress to the user
- queue design requires a loader boundary
- queue design expects optional Virtuous route mounting plus loader-driven drain

### 5. No asset metadata aggregation model

The branch does not model queue item metadata by `job_key -> step_key`.

Why it conflicts:

- queue detail pages need persisted asset-like summaries from successful steps
- partial metadata on failed runs is part of the desired UX

## Practical Reuse Path

If the queue work starts on `main`, the safest reuse path from `feat/partitions` is:

1. Reuse the runtime-context idea for per-run partition metadata.
2. Reuse partition-key normalization helpers where they are generic.
3. Do not adopt the op-level `WithPartition(...)` API for queue ingestion.
4. Do not adopt the backfill schema as the queue schema.
5. Do not adopt the branch UI structure as the queue UI structure.

## Branch-Specific Risks

- The branch is marked `wip`.
- It is large and changes the public DAG DSL, runtime, DB schema, RPC handlers, docs, and frontend all at once.
- It includes generated `frontend-web/dist` changes, which are not a good base for a conceptual queue merge.

## Bottom Line

`feat/partitions` is a partial fit for future queue-adjacent partition context, but not a good direct implementation base for queues.

The right path is:

- implement queues on `main` with queue-owned item tables and loader semantics
- selectively pull over the generic partition runtime helpers later if they still make sense
