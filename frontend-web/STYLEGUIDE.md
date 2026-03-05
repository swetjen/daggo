# Frontend Style Guide (Byodb)

This guide covers how to update the React frontend for byodb.

## Client Usage
- Prefer the generated JS client: `frontend-web/api/client.gen.js`.
- Import from the local file (built by `make gen-sdk`):

```js
import { createClient } from "./api/client.gen.js";
```

- After API changes, run `make gen-sdk` to refresh the local client.

- Instantiate with `window.location.origin` unless you have a proxy.
- Do not hand-roll fetch helpers unless the client is missing a route.

## UI Patterns
- Follow `docs/DAGGO_DESIGN_SYSTEM_V2.xml` (Operational Precision).
- Keep hierarchy driven by layered dark surfaces, borders, and compact spacing.
- Use state accent colors only for runtime/severity (`success`, `error`, `running`, `queued`).
- Avoid Memoria-only effects (BorderBeam, ProgressiveBlur theatrics, 3D glare, starfields).
- Prefer lightweight, dense tables and split panels for list/detail workflows.

## Build & Embed
- After frontend changes: run `make gen-web`.
- Do not edit `frontend-web/dist` manually (generated).

## React Conventions
- Keep request state in component state hooks.
- Normalize error messages and show in `.alert`.
- Re-fetch list views after mutations.
