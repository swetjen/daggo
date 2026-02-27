# Queries Style Guide

- Every query must use sqlc annotations: `-- name: QueryName :one|:many|:exec|:copyfrom`.
- Query names are PascalCase and follow `Domain + Entity + Verb + Qualifier`.
- Canonical verbs: `Get`, `GetBy*`, `GetMany`, `LookupBy*`, `Create`, `Update`, `Upsert`, `Delete`, `Count`.
- Avoid non-canonical verbs like `List`, `Search`, or `SoftDelete`.
- Files are numbered and domain-scoped (match migration epoch naming).
- Use positional args (`$1`, `$2`, ...) and `sqlc.arg/sqlc.narg` for optional filters.
- Return rows from inserts/updates with `RETURNING` for codegen.
- Shape JSON in SQL (`json_agg`, `jsonb_build_object`) and `coalesce` to non-null defaults.
- Use `ON CONFLICT` for upserts with explicit keys.
- Bulk inserts should use `:copyfrom`.
- GetMany queries should accept pagination (`limit`, `offset`, or cursor) and include stable ordering.
- Prefer a paired `Count` query (e.g., `TodoCount`) for list endpoints so clients can paginate; if combining count into a single query makes things brittle or complex, keep it separate.
- Prefer deterministic queries for testing: avoid nullable arguments when possible and use separate queries for distinct behaviors.

# Important!
- After making changes to a query or schema, run `make gen` from the `api` folder.  This uses `sqlc` to perform a code gen and validates the queries are valid with the table schema.
- Fix any errors or conflicts identified by `make gen` before committing code.
- Do not consume the queries manually, only use the Golang code-generated functions created in the `db` package.


See also: `db/sql/SCHEMAS_STYLEGUIDE.md`.
