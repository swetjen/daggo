# PostgreSQL Runtime Spec

This document captures the intended PostgreSQL runtime behavior for DAGGO. The config surface is exposed now, but the runtime implementation is intentionally deferred.

## Status

- Public config exists today under `daggo.Config.Database.Postgres`.
- Runtime support is not implemented yet.
- Current startup returns a clear "not implemented yet" error when `DatabaseDriverPostgres` is selected.

## Goals

- Let DAGGO users provide shared PostgreSQL infrastructure details.
- Provision DAGGO objects into a dedicated schema instead of taking over an entire database.
- Keep SQLite as the zero-config default for local and small deployments.
- Preserve DAGGO's automatic startup flow: open DB, ensure schema, run migrations, serve UI, start scheduler.

## Proposed User Config

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

## Provisioning Model

When PostgreSQL support lands, DAGGO should:

1. Connect to the user-provided database.
2. Create the target schema if it does not already exist.
3. Set `search_path` so all DAGGO tables, indexes, and migration bookkeeping live inside that schema.
4. Run DAGGO migrations into that schema.
5. Leave the rest of the database untouched.

## Migration Expectations

- DAGGO should maintain its own migration ledger table inside the DAGGO schema.
- SQLite and PostgreSQL should share the same logical schema model, but migrations will need engine-specific SQL.
- Startup should remain automatic: no separate migration command required for standard DAGGO boot.

## Connection Behavior

- One config block maps to one PostgreSQL database plus one DAGGO schema.
- The caller owns database creation and credentials.
- DAGGO owns schema bootstrap and migration within that database.
- SSL mode should remain configurable because deployment environments vary.

## Open Questions

- Should DAGGO create the schema with a configurable naming convention when `Schema` is omitted, or should `Schema` remain required?
- Do we want a single sqlc target for both SQLite and PostgreSQL, or a clean split once PostgreSQL SQL diverges?
- Should DAGGO support a database URL form for PostgreSQL in addition to the structured fields?
- What connection pool defaults make sense once DAGGO runs against a network database instead of a local SQLite file?

## Non-Goals For The First Postgres Milestone

- Cross-database orchestration.
- Multi-tenant schema fanout from one process.
- Online schema diff tooling.
- Automatic creation of PostgreSQL roles, databases, or extensions.
