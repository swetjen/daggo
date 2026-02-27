# Changelog

## 2026-02-22
- Replaced the byodb sample domain with DAGGO orchestration primitives.
- Added SQLite schema/queries for jobs, graph metadata, runs, steps, and events.
- Added job registry + DAG validation + DB sync at startup.
- Added in-process goroutine executor with event emission and rerun-step support.
- Added Virtuous RPC handlers for jobs, runs, and schedules.
- Replaced frontend with a DAG observability UI (job graph, run detail, timeline, rerun step).
