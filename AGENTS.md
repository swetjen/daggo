# BYODB AGENTS

Primary index for byodb agents. Start here.

## Styleguides
- `docs/STYLEGUIDES.md` (index)
- `db/sql/SCHEMAS_STYLEGUIDE.md`
- `db/sql/QUERIES_STYLEGUIDE.md`
- `handlers/RPC_STYLEGUIDE.md`
- `frontend-web/STYLEGUIDE.md`

## Release
- `docs/RELEASE_PLAYBOOK.md`

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
- When exposing DAGGO HTTP/RPC/API behavior, use Virtuous as the canonical underlying API layer. Do not add parallel handler or routing abstractions for the same workflow unless explicitly requested.
- Use the generated JS client in the frontend whenever possible.
- Prefer one canonical public API for each user workflow. Do not add parallel entrypoints, synonyms, or alias APIs that do the same thing under different names.
- DAGGO is pre-1.0. When replacing a public API, remove the old path instead of keeping backward-compatibility aliases.
- When a public API changes, update tests, examples, README snippets, and docs so they only show the canonical path.
- `VERSION` at the repo root is the canonical release version and must include the leading `v` (for example `v0.5.0`).
- Git release tags must match `VERSION` exactly. The Go module version is defined by that tag, so treat tag and module version as the same release identifier.
- `frontend-web/package.json` must keep its `version` field in sync with `VERSION` after removing the leading `v`.
- New changelog headings must use `## vX.Y.Z - YYYY-MM-DD`.
- Follow `docs/RELEASE_PLAYBOOK.md` for every DAGGO release.
- The release playbook includes a mandatory downstream validation pass against `/home/incognito/dev/mono/runner`. If runner source changes are needed to accommodate new DAGGO syntax or APIs, pause and ask for approval before editing that repo.
- Before release, run `make gen-all`.
- Before release, validate the README startup snippet in a fresh throwaway Go module or equivalent clean environment, including loading the admin UI and its built module assets.

## Subprocess Worker Safety
- Treat a DAGGO app binary as two distinct runtime modes: the long-lived admin/scheduler process and the `daggo-worker --run-id ...` subprocess.
- Worker subprocess startup must be minimal and deterministic. Before the worker enters DAGGO run execution, it must not run application startup hooks, one-shot ingests, migrations, backfills, schedulers, HTTP servers, queue loaders, deploy monitors, external API calls, or other long-running side effects.
- Downstream apps using `RunDefinitions` or `RunRegistry` may still need to construct job definitions before DAGGO can execute a worker run, but any app-owned startup work outside definition construction must be guarded so it only runs in the server process.
- When adding examples, docs, or app templates that include startup hooks, show an explicit worker-process guard around those hooks.
- When changing worker launch, run creation, scheduler, or startup command handling, add a test proving that a subprocess worker executes the target run directly without running server-only startup work first.
- In subprocess mode, validation should include a real worker invocation path, not only in-process executor tests. A fast manual run should not show unexplained queue delay before `run_started`.
- If a worker exits successfully but the run failed immediately, inspect whether app-owned setup closed or invalidated dependencies before step execution. Errors like `closed pool`, stale DB handles, or 1-5 ms step failures are signals to audit worker startup side effects.
