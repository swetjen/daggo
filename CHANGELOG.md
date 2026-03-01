# Changelog

## 2026-03-01
- Simplified the public runtime API to `daggo.Run(...)` and `daggo.Open(...)` as the only canonical startup paths for jobs.
- Removed the old `daggo.Main(...)`, `daggo.NewApp(...)`, `daggo.WithJobs(...)`, and `daggo.WithRegistry(...)` entrypoints.
- Added explicit advanced startup paths `daggo.RunRegistry(...)` and `daggo.OpenRegistry(...)` for registry-driven integration.
- Added optional bearer-secret protection for `/rpc/` and `/rpc/docs/` via `cfg.Admin.SecretKey` / `DAGGO_ADMIN_SECRET_KEY`.
- Updated generated RPC clients to send `Authorization: Bearer <secret>` when auth options are provided.
- Documented the subprocess runner model, secure no-UI deployment mode, and the recommended `jobs/`, `ops/`, and `resources/` imported-app layout.
- Refreshed the README with UI screenshots, a richer example job graph, and clearer ops/dependency examples.
- Made `dag.ScheduleDefinition.Key` optional and derive readable schedule keys from cron expressions by default.
- Moved current schedule source-of-truth to the in-memory startup registry instead of persisted `job_schedules` rows.
- Persisted only scheduler runtime bookkeeping by `(job_key, schedule_key)` for dedupe and next-run tracking.
- Preserved historical runs when jobs or schedules disappear from the current registry.
- Removed the legacy `job_schedules` table and its query/store surface from SQLite and PostgreSQL.
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
