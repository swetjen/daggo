# Schemas Style Guide

- Use schemas to organize **concepts** for this project; avoid a generic `core` bucket.
  - Examples: `account`, `knowledge`, `internal` (only if needed), or project-specific domains like `billing`, `catalog`, `reports`.
- **Strongly avoid NULLs.** Always prefer explicit defaults with `NOT NULL`.
  - `NOT NULL` is only acceptable without a default if a true third state is required.
  - Choose a sensible default state most of the time (empty string/array/object, 0, false, epoch).
- IDs should use stable prefixes where helpful (e.g., `org_`, `cli_`, `usr_`, `todo_`), but keep the rule general.
- Prefer `jsonb` for flexible payloads; always default to `{}` or `[]`.
- Always add foreign keys and `ON DELETE CASCADE` where lifecycle is owned by the parent.
- Use `timestamptz` for timestamps; default `now()`.
- Avoid table partitions unless explicitly requested.
- Add search indexes: trigram `GIN` for text search, `GIN` on JSONB, and targeted partial indexes.
- Enums are allowed for small, stable state machines; otherwise use lookup tables.
- Extensions used: `pgcrypto`, `pg_trgm`, `btree_gin`; no others without review.
- Migrations must be `goose`-style with `-- +goose Up/Down` and be reversible.
- Prefer **multiple smaller migrations** over one huge migration.
  - Multiple concepts per migration are OK when they share a clear theme.
  - If changes cross a theme boundary, split them into separate migrations.
- Avoid `IF NOT EXISTS` on `CREATE TABLE`. Migrations should fail if a table already exists to surface schema drift early.
- Prefer schemas that support deterministic pagination (indexed `created_at` or `id` fields for stable ordering).

See also: `db/sql/QUERIES_STYLEGUIDE.md`.
