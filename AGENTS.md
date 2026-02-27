# BYODB AGENTS

Primary index for byodb agents. Start here.

## Styleguides
- `docs/STYLEGUIDES.md` (index)
- `db/sql/SCHEMAS_STYLEGUIDE.md`
- `db/sql/QUERIES_STYLEGUIDE.md`
- `handlers/RPC_STYLEGUIDE.md`
- `frontend-web/STYLEGUIDE.md`

## Canonical Agent Flow
1) Update schema + queries in `db/sql/schemas` and `db/sql/queries`.
2) Run `make gen`.
3) Implement RPC handlers in `handlers/`.
4) Run `make gen-sdk`.
5) Wire React UI using the generated JS client.
6) Run `make gen-web` (or `make gen-all`).

## Guardrails
- You must follow the linked styleguides for any change in their domain.
- Never edit sqlc outputs, generated SDKs, or `frontend-web/dist` manually.
- Never edit `frontend-web/api/client.gen.js` by hand (generated).
- Do not modify files under `frontend-web/api/` manually (generated).
- Use the generated JS client in the frontend whenever possible.
- Before release, run `make gen-all`.
