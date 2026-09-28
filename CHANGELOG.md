# Changelog

### Unreleased
- Added an unauthenticated `GET /healthz` endpoint. It returns `200` with a small JSON body (overall status, database reachability, scheduler state, draining flag) and `503` when the database is unreachable or the scheduler is enabled but not alive. It needs no bearer token when `DAGGO_ADMIN_SECRET_KEY` is set and is served when `DAGGO_DISABLE_UI=true`.
- Added graceful shutdown on `SIGTERM` and `SIGINT` for `daggo.Run(...)`, `daggo.RunRegistry(...)`, and `daggo.RunDefinitions(...)`. The signal starts the existing deploy drain: new runs are refused, the scheduler stops creating runs, in-flight runs get `DEPLOY_DRAIN_GRACE_SECONDS` to finish, HTTP shuts down, and the process exits `0`. A second signal ends the grace period immediately. Previously these signals killed the server at once and left its runs in `running`.
- Changed the end of the drain grace period, for both the signal and the `DEPLOY_LOCK_PATH` lock file: runs still in flight are now stored with status `failed`, error message `interrupted by shutdown`, and a `run_interrupted` event, and their worker processes are terminated instead of being left running after the server exits. No new run status was added.
- Changed subprocess workers to run in their own process group, so a signal sent to the server's process group (for example Ctrl+C in a terminal) no longer reaches workers directly, and terminating a run also terminates processes its worker spawned.
- Changed `RUN_MAX_CONCURRENT_RUNS` / `cfg.Execution.MaxConcurrentRuns` to apply in `subprocess` mode, where it was ignored. It now caps the number of concurrent worker processes; further runs wait in arrival order, record a `run_worker_waiting` event, and start as slots free up.
- Changed the default run concurrency so existing subprocess deployments do not become serial. Unset now means `8` in `subprocess` mode and `1` in `in_process` mode; `daggo.DefaultConfig()` returns `0` (unset) for `Execution.MaxConcurrentRuns` instead of `1`. An explicitly set value is honored, so a deployment that already sets `RUN_MAX_CONCURRENT_RUNS=1` in `subprocess` mode now runs one worker at a time. `.env.example` used to ship that line; remove it from any `.env` copied from the template unless serial execution is intended.
- Added a PostgreSQL advisory lock around schema creation and startup migrations, so processes starting at the same time against an empty schema all succeed. Each migration is now applied and recorded in one transaction, and a process that finds the schema up to date starts without taking the lock. SQLite is unchanged.
- Fixed run retention never purging `scheduler_schedule_runs`. With `RUN_RETENTION_DAYS` set, scheduler schedule-run records older than the window are purged once the run they created is gone, alongside runs, run steps, and run events. Runs that have not finished are never purged.
- Changed the admin bearer check to compare tokens in constant time with `crypto/subtle`.
- Added `SchedulerScheduleRunGetManyForRetentionPurge` to the `db.Store` interface; custom `db.Store` implementations need the new method.
- Added `DAGGO_TEST_POSTGRES_DSN` for the PostgreSQL-only tests, which skip when it is unset.

## v0.6.3 - 2026-06-18
- Raised Go security dependency floors for `golang.org/x/net`, `google.golang.org/grpc`, related `golang.org/x/*` modules, and the Go toolchain patch level.

## v0.6.2 - 2026-06-07
- Updated `github.com/swetjen/virtuous` from `v0.0.36` to `v0.0.54`.

## v0.6.1 - 2026-05-29
- Updated `github.com/swetjen/virtuous` from `v0.0.17` to `v0.0.36`.

## v0.6.0 - 2026-05-28
- Added a dedicated Overview snapshot RPC and admin timeline that balances recent runs across jobs, shows schedule/run activity, and surfaces window-level run stats.
- Added server-side run filtering, quick filters, cursor pagination, and sortable run history for SQLite and PostgreSQL.
- Added `daggo.CurrentProcess()` with `ProcessModeServer` and `ProcessModeWorker` so imported apps can guard server-only startup work from subprocess workers.
- Added a real subprocess regression test proving `daggo-worker --run-id ...` executes a run without running guarded server startup work.
- Documented worker-safe startup layout for `RunDefinitions(...)`, embedded `OpenDefinitions(...)`, and Dagster migration scenarios.

## v0.5.2 - 2026-03-25
- Added a disabled-by-default `cfg.Retention.RunDays` / `RUN_RETENTION_DAYS` setting for automatic daily purge of terminal run history older than the configured retention window.
- Added runtime retention cleanup for old runs, cascading run artifacts, and aged-out queue items whose linked runs have all been purged.
- Surfaced retention settings in the admin Settings page and system settings RPC snapshot.

## v0.5.1 - 2026-03-24
- Added a read-only Settings page in the admin UI footer so operators can inspect normalized DAGGO configuration from the browser.
- Added a sanitized `system.SettingsGet` RPC that exposes admin, database, execution, scheduler, and deploy settings without returning secrets.

## v0.5.0 - 2026-03-22
- Added a canonical root `VERSION` file and surfaced the running DAGGO version in the admin UI footer, runtime RPC info, and startup banner.
- Added version sync checks so release tags, the Go module release version, `VERSION`, the frontend package version, and new changelog entries stay aligned.

## 2026-03-07
- Added light mode with a top-level theme toggle, persistent user preference, and first-load system theme detection while preserving existing dark mode layout/density.
- Redesigned the Jobs list into a denser operational table with aligned columns, inline enable/disable switches, and simplified row actions.
- Added run-health histogram tile hover popovers with status, timestamp, duration, and direct navigation to run detail.
- Updated run-health tile ordering to render oldest on the left and newest on the right.
- Simplified job detail controls by using the shared iOS-style scheduling switch and removing redundant header labeling.
- Tightened DAG node and run-step visual legibility by reducing transparency and strengthening state fills across job/run detail canvases.
- Updated README with planned roadmap items (worker pool/admin coordination, partitions/assets, migration support, and reference integrations).
- Cleaned up frontend assets by removing stale hashed `dist` bundles and wiring the build to clear old `index-*` outputs before generating new assets.
- Removed unused root-generated SDK artifacts (`client.gen.ts`, `client.gen.py`) and kept SDK generation focused on the frontend JS client.
- Bumped the frontend package minor version to `0.5.0`.

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
