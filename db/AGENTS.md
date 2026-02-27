# BYODB DB AGENTS

These instructions apply to the database layer in `example/byodb/db`.

## Goals
- Use sqlc for all DB access. Do not hand-write query code outside of sqlc output.
- Keep schemas and queries in `db/sql/schemas` and `db/sql/queries`.

## Required Workflow
- Add or update SQL in `db/sql/schemas` and `db/sql/queries`.
- Run `make gen` after SQL changes to regenerate sqlc output.
- Run `make gen-sdk` after adding or adjusting API routes to refresh client SDKs.
- Run `make gen-web` after frontend changes to rebuild embedded assets.
- Run `make gen-all` before release to regenerate SQL, SDKs, and frontend assets together.
- Do not manually edit sqlc outputs or generated SDKs or `frontend-web/dist`.
- You must follow the linked styleguides for any change in this domain.
- Only use the generated Go methods in the `db` package.

## Style Guides
- Follow `db/sql/QUERIES_STYLEGUIDE.md`.
- Follow `db/sql/SCHEMAS_STYLEGUIDE.md`.

## Notes
- This example expects PostgreSQL and uses `pgx`.
- `DATABASE_URL` is required to run the API.
