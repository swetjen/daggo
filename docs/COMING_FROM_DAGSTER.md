# Coming From Dagster

This document maps Dagster concepts to DAGGO and highlights where behavior is intentionally different.

## Concept Mapping

- Dagster `op` / `solid` -> DAGGO `step` (`dag.Define[I, O]`)
- Dagster `job` -> DAGGO `job` (`dag.NewJob(...).MustBuild()`)
- Dagster schedule -> DAGGO `ScheduleDefinition` with cron + timezone
- Dagster run -> DAGGO run record with step/event history
- Dagster run logs/events -> DAGGO `run_events` + run detail event feed

## Core Philosophy Differences

Dagster is graph-defined with explicit dependency APIs.

DAGGO is type-defined:
- Dependencies are inferred from step input/output types.
- Cardinality/optionality is encoded by field shape (`T`, `[]T`, `*T`).
- Graph validation fails at build time for missing/ambiguous providers.

There is no string-based wiring and no override-preference API.

## Input Wiring Rules

- `T`:
  - exactly one producer required
  - zero producers = build error
  - multiple producers = build error
- `[]T`:
  - inject all producers (deterministic topological order)
  - zero producers allowed
- `*T`:
  - zero or one producer
  - zero injects `nil`
  - multiple producers = build error

## Operational Model

- Single binary serves API + embedded UI.
- Runs execute asynchronously (default detached subprocess workers).
- Scheduler is built-in and cron-driven.
- Persistent state is local SQLite by default.

This keeps setup simple while retaining observability similar to Dagster’s operator workflow.

## UI Model

DAGGO uses an operator-focused shell:
- `Overview`: temporal activity and schedule cadence
- `Jobs`: structural inventory and launch point
- `Runs`: run history and filterable diagnostics
- Run detail: graph + execution/event diagnostics

## Current Gaps Relative to Dagster

- No Dagster compatibility target (DB, GraphQL, or UI parity).
- Asset lineage view is not a v1 focus.
- Backfills/partitions and richer deployment topologies are incremental work.
- Multi-tenant orchestration is not a current goal.

## Migration Approach

1. Re-model each Dagster pipeline/job as DAGGO typed steps.
2. Encode dependencies through input/output structs.
3. Attach schedules with explicit cron/timezone.
4. Validate build-time graph errors and ambiguity early.
5. Use run detail/event feed for debugging and operator workflows.

## Practical Notes

- You can keep runtime params minimal; DAGGO is optimized for code-defined jobs.
- Prefer deterministic outputs and explicit step keys.
- Keep job behavior stable under code changes by preserving step/output types.

## Package Direction

DAGGO is intended to be imported into your own Go application:

- define jobs in your application code
- configure DAGGO through `daggo.Config`
- start the runtime with `daggo.Main(...)` or `daggo.NewApp(...)`
