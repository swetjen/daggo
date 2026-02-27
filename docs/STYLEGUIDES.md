# Byodb Styleguide Index

This index links to all styleguides used by the byodb example. Agents should read the relevant guide before modifying a domain.

## Database
- `db/sql/SCHEMAS_STYLEGUIDE.md`
- `db/sql/QUERIES_STYLEGUIDE.md`

## RPC (API)
- `handlers/RPC_STYLEGUIDE.md`

## Frontend
- `frontend-web/STYLEGUIDE.md`

## Agent Flow (Recap)
1) Update schema + queries in `db/sql/schemas` and `db/sql/queries`.
2) Run `make gen`.
3) Implement RPC handlers in `handlers/`.
4) Run `make gen-sdk`.
5) Wire React UI using the generated JS client.
6) Run `make gen-web` (or `make gen-all`).
