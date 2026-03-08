# Change Log Notes (Partitions Rollout)

Use this file to capture user-facing release notes as implementation lands.

## Unreleased

### Added
- Initialized partition implementation tracking artifacts (`partition_progress.md`, `change_log_notes.md`).
- Added partitions/backfills schema foundations for SQLite and Postgres:
  - `partition_definitions`
  - `partition_keys`
  - `run_partition_targets`
  - `run_system_tags`
  - `backfills`
  - `backfill_partitions`
- Added sqlc query surface for:
  - partition definition/key upsert and lookup
  - range/subset partition key retrieval
  - run partition target and system tag persistence
  - backfill lifecycle and per-partition status accounting
- Added typed partition domain model in `dag/`:
  - `PartitionDefinition` contract with static, dynamic, time-window, and multi-dimension implementations
  - `PartitionSelection` clause model supporting single, range, subset, and merged non-contiguous selections
  - `NormalizePartitionSelection*` APIs that produce canonical selected keys and contiguous range segments
- Added run-target and launch-planning primitives in `dag/`:
  - run-target tag rendering with projection strategies (`daggo`, `dual`, `dagster`)
  - run-target tag parsing back into typed selection semantics
  - materialization planning that defaults to one run per selected partition
  - backfill planning with `single_run` and chunked `multi_run` policies
- Added backfill status RPC surface:
  - `backfills.BackfillsGetMany`
  - `backfills.BackfillByKey`
  - `backfills.BackfillLaunch`
  - router wiring under `/rpc/backfills/*`
  - handler-set registration for backfill domain
- Added partition mapping + lineage primitives in `dag/`:
  - `ResolveUpstreamMappedPartitionsForSelection` API
  - mapping implementations: `IdentityPartitionMapping`, `StaticPartitionMapping`, `AllPartitionMapping`, `LastPartitionMapping`, `TimeWindowPartitionMapping`, `MultiPartitionMapping`
  - explicit `RequiredButNonexistentKeys` result field for dependency gap reporting
- Added Job detail UI backfill controls:
  - selection modes (`all`, `single`, `range`, `subset`)
  - launch policy controls (`multi_run`, `single_run`, max partitions per run)
  - recent backfill list with status chips and inline partition status detail panel

### Changed
- Locked partitions D3 strategy: typed canonical partition/backfill target model with pluggable tag projection (DAGGO-first tags; Dagster-compatible projection can be added without core model changes).
- Generated DB models/query code (`make gen`) to include partition/backfill query primitives.
- Updated store abstraction and postgres adapter with backfill query methods required by status handlers.
- Regenerated SDK clients (`make gen-sdk`) to include new backfill RPC routes.
- Extended launch flow to persist run partition targets + system tags and map backfill partition subsets to requested runs.
- Added launch guardrails:
  - partition definition key count cap
  - selection size cap
  - max planned run count cap
  - max `max_partitions_per_run` cap
- Added structured launch logs for normalized selections and launch outcomes.

### Fixed
- _None yet._

### Docs
- Added partitions planning decision log updates (`partition_progress.md`) and tag-strategy evaluation note (`docs/PARTITIONS_TAG_STRATEGY_STRAWMAN.md`).
- Updated partition progress tracker to mark Chunk 1 complete.

### Testing
- Added SQLite tests for partition/backfill migration/query lifecycle:
  - migration application assertion for `006_partitions_backfills.sql`
  - partition key subset/range retrieval
  - run target + system tag persistence
  - backfill partition subset accounting and status updates
- Added partition domain unit tests for FR-1/FR-2 acceptance behavior:
  - static key enumeration
  - dynamic partition store requirement errors
  - multi-partition dimension-count guardrails
  - range validity and unknown endpoint failures
  - subset validity failures
  - non-contiguous merged selection normalization
- Added launch/tag tests for Chunk 3 behavior:
  - single/range/subset tag projection behavior
  - dagster projection guardrails for non-contiguous subset targets
  - materialization one-run-per-partition planning
  - backfill single-run and chunked multi-run planning (including non-contiguous selections)
- Added backfill handler tests for:
  - list/status summary retrieval by job key
  - completion/accounting calculations from partition subset statuses
  - per-backfill partition detail retrieval by backfill key
  - backfill launch flow creates runs, run targets, and requested partition subset assignments
- Added operational workflow test for backfills:
  - launch via range selection with chunked multi-run policy
  - verify list/detail status retrieval
  - simulate partition materialization/failure updates
  - assert terminal completion accounting in summaries
- Added partition mapping tests for:
  - identity mapping with missing upstream partitions surfaced explicitly
  - static mapping behavior and missing-key reporting
  - time-window mapping behavior with missing upstream windows surfaced
  - multi-dimensional mapping behavior (default identity and explicit dimension mapping)
