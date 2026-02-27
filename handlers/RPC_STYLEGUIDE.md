# RPC Style Guide (Byodb)

This guide covers how to add or update Virtuous RPC handlers in the byodb example.

## Handler Rules
- Handlers are plain functions: `func(ctx, req) (resp, status)` or `func(ctx) (resp, status)`.
- Status is **one of**: `rpc.StatusOK`, `rpc.StatusInvalid`, `rpc.StatusError`.
- Do not return `error`; encode errors into the response payload.
- Canonically expose errors via an `error` field in the response payload; the field can be a string, struct, or another shape that best represents the error for your use case.

## Naming + Routing
- Route paths are inferred from **package + function name**.
- Keep handler functions **named** (no inline lambdas).
- Package names become service names in generated clients.
- Namespace carefully: keep packages scoped to a clear domain, but don’t over-fragment.
- If a namespace grows to ~20+ routes, consider splitting into sub-namespaces (e.g., `users` → `users/admin`, `users/profile`, or `users/auth`) to keep clients navigable.
- Canonical naming:
  - `GetMany` for list endpoints.
  - `GetBy<Id|Name|Email|Code|...>` for lookup endpoints.
  - `Create`, `Update`, `Delete` for lifecycle endpoints.
  - Use `DeleteBy<...>` when there are multiple delete paths.
  - Prefer `Upsert` when you truly need insert-or-update.
  - Include pagination (`limit`, `offset` or cursor) on all `GetMany` routes.
  - For `GetMany`, include a total row count when feasible so clients can paginate intelligently.
  - Avoid non-canonical verbs like `List` in handler names.

Examples:
- `states.StatesGetMany` → `/rpc/states/states-get-many`
- `states.StateByCode` → `/rpc/states/state-by-code`
- `admin.UserCreate` → `/rpc/admin/user-create`

## DB Integration
- Call sqlc-generated methods directly (`db.*`), no manual SQL.
- Validate input before calling DB.
- Use `rpc.StatusInvalid` for validation errors.
- Use `rpc.StatusError` for DB failures (except not found, which is Invalid).
- Treat `no rows` as a successful 200 with zero data where appropriate (e.g., list endpoints). Clients should handle empty results gracefully (e.g., show a “no rows found” message).
- Ensure `GetMany` responses use stable ordering and include total counts when available (from a paired `Count` query).

## Guards
- Attach guards in the router via `HandleRPC(fn, guard)`.
- Guards are **not** used inside handler logic.

## Generate SDKs
- After adding or changing handlers, run `make gen-sdk`.
- Do not edit generated clients manually.
