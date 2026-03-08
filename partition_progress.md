# Partition Implementation Progress (DAGGO)

Branch: `feat/partitions`
Source plan: `partition_plan.md`
Last updated: 2026-03-08

## Current Status
- Phase: `Chunk 6 Complete`
- Overall: `100%`

## Strategic Decisions (Required Before Build)
- [x] D1: Confirm domain model direction.  
  Decision: implement v1 on partitioned **jobs/steps** with asset metadata/events, not a separate asset engine rewrite.
- [x] D2: Confirm v1 scope boundaries.  
  Decision: proceed with full plan scope, delivered in phased chunks.
- [x] D3: Confirm run targeting contract and tag strategy.  
  Decision: use a typed canonical partition-target model, with tags as projections. Support dual-tag emission via projection strategy (DAGGO tags now; Dagster-compatible projection can be enabled without changing core semantics).
- [x] D4: Confirm launch semantics for v1 backfills.  
  Decision: unit of work should follow job definition; partitioned/asset-aware jobs define whether execution is per-partition or range/chunk.
- [x] D5: Confirm API surface (RPC endpoints + request/response contracts) for partition selection, backfill launch, and status polling.  
  Decision: backend/API-first, then UI wiring once APIs stabilize.

## Decision Log
- 2026-03-07: Domain model direction locked: extend current jobs/steps model for partitions and asset-level events.
- 2026-03-07: Scope approved for full partition/backfill plan (phased implementation).
- 2026-03-07: Tag strategy locked: typed canonical model + pluggable tag projection layer (not tag-first semantics).
- 2026-03-07: Launch semantics to be definition-driven (job/asset partition config determines sensible run unit).
- 2026-03-07: Execution order approved as backend-first followed by UI integration.
- 2026-03-08: Chunk 1 delivered: schema + query foundations for partitions/backfills, sqlc generation clean, SQLite lifecycle tests and migration embedding checks added.
- 2026-03-08: Chunk 2 delivered: typed partition definitions (static/time/dynamic/multi), selection clause normalization (single/range/subset + merged/non-contiguous), and FR-1/FR-2 style unit tests.
- 2026-03-08: Chunk 3 delivered: run-target tag projection/parsing, materialization launch planning (one-run-per-partition baseline), and backfill plan builder (`single_run` + chunked `multi_run`) with launch/tag tests.
- 2026-03-08: Chunk 4 delivered: backfill subset status retrieval + completion accounting in RPC handlers, router wiring, sdk regeneration, and handler tests for partition state reporting.
- 2026-03-08: Chunk 5 delivered: partition mapping resolution APIs (identity/time-window/static/multi) with explicit `required_but_nonexistent` partition subsets and mapping test coverage.
- 2026-03-08: Chunk 6 delivered end-to-end: guardrails + structured logs in launch path, admin UI wiring for launch + status in Job detail, and operational workflow tests covering launch/list/detail/completion accounting.
- 2026-03-08: Chunk 7 docs updates delivered: README, usage guide, and Dagster migration notes updated to reflect current partition/backfill rollout state.

## Implementation Chunks

### Chunk 1: Data Model + Schema
- [x] Add partition/backfill tables and indexes (sqlite + postgres schemas).
- [x] Add sql queries for partition keys, selection subsets, run targeting, and backfill status.
- [x] Run `make gen` and validate generated store APIs.
- [x] Add migration tests for sqlite and postgres.

### Chunk 2: Core Domain Model + Validation
- [x] Implement `PartitionSelection` normalization (single/range/subset).
- [x] Implement partition definition adapters (static/time first; dynamic/multi by decision).
- [x] Implement selection validation errors and normalization outputs.
- [x] Add unit tests for FR-1/FR-2 acceptance criteria.

### Chunk 3: Run Targeting + Launch Primitives
- [x] Implement run-target → tag rendering and parsing.
- [x] Implement launch materialization for partition selections (N partitions → N runs baseline).
- [x] Implement backfill plan builder (`single_run` vs chunked `multi_run`).
- [x] Add run/backfill launch tests and tag correctness tests.

### Chunk 4: Backfill State Machine + Status APIs
- [x] Persist backfill state and subsets (`target`, `requested`, `materialized`, `failed/downstream`).
- [x] Implement polling/completion logic and chunked submission behavior.
- [x] Expose status retrieval endpoints in RPC handlers.
- [x] Add tests for completion/accounting and chunk controls.

### Chunk 5: Partition Lineage + Mappings
- [x] Implement partition mapping resolution APIs.
- [x] Surface `required-but-nonexistent` upstream subsets explicitly.
- [x] Add mapping tests for identity/time-window/static/multi mapping behavior.

### Chunk 6: Guardrails + Observability + UI Integration
- [x] Add partition-count, backfill-size, and rate/concurrency guardrails.
- [x] Add structured logs/metrics around selection, launches, and backfill transitions.
- [x] Wire admin UI for partition selection, backfill launch, and status.
- [x] Add end-to-end tests for operational workflows.

### Chunk 7: Docs + Migration Guidance
- [x] Update README and `docs/USAGE_GUIDE.md` with partition/backfill flows.
- [x] Update `docs/COMING_FROM_DAGSTER.md` for feature parity caveats.
- [x] Add rollout notes from `change_log_notes.md`.

## Risks / Watch Items
- Dynamic partition range semantics can be ambiguous.
- Single-run backfills require strict runtime + IO manager compatibility.
- Cross-product explosion for multi-dimensional partitions.
- Backfill accounting consistency under retries/failures.

## Notes
- Update this file at the end of each completed chunk.
- Add user-facing highlights to `change_log_notes.md` as soon as each chunk lands.
