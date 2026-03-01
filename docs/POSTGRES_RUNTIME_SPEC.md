# PostgreSQL Runtime

This document covers how DAGGO uses PostgreSQL today.

## Summary

- PostgreSQL is supported.
- SQLite is still the default.
- PostgreSQL only activates when the driver is explicitly set to `postgres`.
- DAGGO provisions into a user-provided schema inside a user-provided database.

## Required Config

Code configuration:

```go
cfg := daggo.DefaultConfig()
cfg.Database.Driver = daggo.DatabaseDriverPostgres
cfg.Database.Postgres.Host = "db.internal"
cfg.Database.Postgres.Port = 5432
cfg.Database.Postgres.User = "daggo"
cfg.Database.Postgres.Password = "secret"
cfg.Database.Postgres.Database = "platform"
cfg.Database.Postgres.Schema = "customer_a_daggo"
cfg.Database.Postgres.SSLMode = "require"
```

Environment configuration:

```bash
export DAGGO_DATABASE_DRIVER=postgres
export DAGGO_POSTGRES_HOST=db.internal
export DAGGO_POSTGRES_PORT=5432
export DAGGO_POSTGRES_USER=daggo
export DAGGO_POSTGRES_PASSWORD=secret
export DAGGO_POSTGRES_DATABASE=platform
export DAGGO_POSTGRES_SCHEMA=customer_a_daggo
export DAGGO_POSTGRES_SSLMODE=require
```

## Startup Behavior

When PostgreSQL is selected, DAGGO startup does the following:

1. Connects to the configured PostgreSQL database.
2. Creates the configured schema if it does not exist.
3. Reconnects with `search_path` set to `<daggo_schema>,public`.
4. Creates the DAGGO migration ledger table if needed.
5. Runs embedded up-migrations automatically.
6. Uses PostgreSQL for jobs, runs, scheduler state, and events.

This means the caller owns:

- the PostgreSQL server
- the database
- the credentials

DAGGO owns:

- the DAGGO schema inside that database
- DAGGO tables and indexes
- DAGGO startup migrations

## Important Constraints

- PostgreSQL is explicit opt-in. Setting PG env vars without `DAGGO_DATABASE_DRIVER=postgres` does not switch the runtime away from SQLite.
- `Database.Postgres.Schema` is required.
- DAGGO expects a simple PostgreSQL schema identifier and rejects invalid schema names.
- DAGGO provisions inside one schema per config block; it does not manage multiple schemas from one runtime instance.

## SQL / Codegen Layout

The SQL and generated packages are split by engine:

- SQLite SQL: `db/sql/sqlite`
- PostgreSQL SQL: `db/sql/postgres`
- SQLite generated package: `db`
- PostgreSQL generated package: `db/postgresgen`

Runtime code uses a common `db.Store` boundary so SQLite and PostgreSQL can share the rest of the DAGGO runtime.

## Current Gaps

- PostgreSQL integration tests are not in place yet.
- There is not yet a PostgreSQL URL-style config field; the runtime currently uses structured connection fields.
- Migration locking for concurrent startup is not implemented yet.
- Connection pool tuning is still minimal.

## Recommended Next Improvements

1. Add disposable PostgreSQL integration tests covering migrations, job sync, run creation, and scheduler flows.
2. Add migration locking so two DAGGO processes cannot race schema upgrades.
3. Add optional PostgreSQL URL support if callers want to provide a single DSN instead of structured fields.
