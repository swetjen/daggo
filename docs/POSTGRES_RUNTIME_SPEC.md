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
cfg.Database.Postgres.Schema = "my_project"
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
export DAGGO_POSTGRES_SCHEMA=my_project
export DAGGO_POSTGRES_SSLMODE=require
```

## Startup Behavior

When PostgreSQL is selected, DAGGO startup does the following:

1. Connects to the configured PostgreSQL database with `search_path` set to `<daggo_schema>,public`.
2. Checks the DAGGO migration ledger. When every embedded migration is already recorded, startup continues at step 6.
3. Takes a PostgreSQL advisory lock scoped to the schema name, on a separate connection.
4. Creates the configured schema and the DAGGO migration ledger table if they do not exist.
5. Runs pending embedded up-migrations, each one applied and recorded in a single transaction, then releases the lock.
6. Uses PostgreSQL for jobs, runs, scheduler state, and events.

Processes that start at the same time against the same schema take turns at steps 3 to 5, so all of them succeed, including against an empty schema.

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

- PostgreSQL integration tests cover concurrent startup migrations and run retention only, and run only when `DAGGO_TEST_POSTGRES_DSN` points at a throwaway server.
- There is not yet a PostgreSQL URL-style config field; the runtime currently uses structured connection fields.
- Connection pool tuning is still minimal.

## Recommended Next Improvements

1. Extend the disposable PostgreSQL integration tests to job sync, run creation, and scheduler flows.
2. Add optional PostgreSQL URL support if callers want to provide a single DSN instead of structured fields.
