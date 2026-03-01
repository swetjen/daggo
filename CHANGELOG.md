# Changelog

## 2026-03-01
- Reduced the runtime RPC surface to docs and OpenAPI only; the app no longer serves live JS/TS/Python generated client routes.
- Removed the `RPC base` startup banner line and kept the console output focused on the admin UI and RPC docs.
- Shipped built frontend assets with the module so imported DAGGO apps load the admin UI without requiring local frontend builds.
- Fixed file-backed SQLite startup for fresh projects by creating parent directories before applying migrations.
- Added startup console connection hints for the admin UI and RPC docs.
- Released DAGGO as an importable Go runtime package with `daggo.Main`, `daggo.NewApp`, and public config.
- Added explicit PostgreSQL runtime support with schema bootstrap and automatic startup migrations.
- Split SQL/sqlc generation by engine and introduced a shared DB store boundary for SQLite and PostgreSQL.
- Updated README and usage docs with SQLite and PostgreSQL startup instructions.

## 2026-02-22
- Replaced the byodb sample domain with DAGGO orchestration primitives.
- Added SQLite schema/queries for jobs, graph metadata, runs, steps, and events.
- Added job registry + DAG validation + DB sync at startup.
- Added in-process goroutine executor with event emission and rerun-step support.
- Added Virtuous RPC handlers for jobs, runs, and schedules.
- Replaced frontend with a DAG observability UI (job graph, run detail, timeline, rerun step).
