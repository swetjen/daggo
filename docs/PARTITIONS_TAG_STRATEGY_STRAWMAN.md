# Partitions Tag Strategy Strawman

This note evaluates `DAGGO-only tags` versus Dagster-compatible tag keys for partition/backfill run targeting.

## Goal
Separate concerns that are mostly semantic from concerns that create real user friction.

## Option Under Review
- Use DAGGO-native system tags only (for example `daggo/partition`, `daggo/partition_range_start`, `daggo/backfill`).
- Do not emit Dagster-compatible keys (for example `dagster/partition`, `dagster/backfill`).

## Cons: Semantic vs Real User Impact

| Concern | Semantic Only? | Real User Problem? | Why it matters |
|---|---|---|---|
| Naming mismatch with Dagster docs/examples | Mostly semantic | Low | Users can relearn names; docs can map equivalents. |
| Loss of copy/paste compatibility from Dagster automation and examples | No | Medium | Migration scripts, runbook snippets, and tooling assumptions break unless translated. |
| Third-party tooling expecting Dagster keys | No | Medium to High | External dashboards/ops scripts that parse standard keys will not recognize DAGGO runs. |
| Mixed fleets (Dagster + DAGGO) operational consistency | No | High | Shared SRE workflows become harder when the same concept has two tag namespaces. |
| Internal storage/API future flexibility | Not a con; a pro | N/A | DAGGO-only can simplify internal evolution if tags are treated as boundary output only. |
| User ability to reason about run targeting in UI/API | Depends on UX | Low to Medium | If UI hides tags well, impact is low; if users inspect tags directly, mismatch increases confusion. |

## Bottom Line
- `DAGGO-only tags` is not just semantics. It creates real migration and operations friction in mixed or migrating environments.
- For greenfield DAGGO-only users, impact is smaller and mostly ergonomic.

## Recommended Direction
- Keep an internal canonical run-target model (typed fields in code/data model).
- At API/event boundaries, support Dagster-compatible system tags for interoperability.
- Optionally emit DAGGO aliases for readability, but treat one set as canonical for behavior.
- Protect system tag keys from user override regardless of namespace.

## Decision
- D3 locked: typed canonical partition/backfill targeting in DAGGO core.
- Tags are emitted from a projection strategy:
  - DAGGO tags first for v1.
  - Dagster-compatible projection can be added/enabled later without changing internal semantics or APIs.
