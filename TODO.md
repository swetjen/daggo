# DAGGO TODO

This file tracks intentionally deferred work after the current MVP.

## Execution Model

- Add strongly-typed step input/output contracts with compile-time checks.
- Add richer retry policies (max retries, backoff, jitter, retryable error classes).
- Add cancellation and termination API for active runs.
- Add per-step resource constraints and concurrency groups.

## Orchestration Features

- Add true scheduler loop (cron trigger execution) with persisted heartbeats.
- Add partitions/backfills and run slicing by partition key.
- Add sensors/event-driven triggers.
- Add materialization/asset concepts beyond simple step outputs.

## Deployment Topology

- Split in-process executor into dedicated daemon service.
- Add durable run queue and worker leasing for multi-process execution.
- Add leader election for scheduler/daemon components.

## Observability

- Add server-side event streaming (SSE/WebSocket) to replace polling.
- Add step log tailing and structured log filters.
- Add run metrics (queue latency, step duration histograms, failure rates).

## UI

- Run comparison view (parent run vs rerun step lineage).
- Schedule management UI (create/update/disable).
- Filter and search for runs/events across jobs.
- Add row virtualization for Overview timeline at high job counts.
- Move Overview time-window filtering to server-side queries (avoid large client-side scans).

## Storage

- Add run retention policies and archival for old events/logs.
- Add output payload offloading for large artifacts.
- Add schema for run tags and indexed query predicates.
