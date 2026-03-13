# Partitions and Backfills (Experimental)

This guide documents the current partition/backfill contract in DAGGO as of March 9, 2026.

## Canonical Model

- Partitions are attached to ops, not jobs.
- A job is considered partitioned when at least one op has a partition definition.
- During startup sync, DAGGO writes op partition metadata into:
  - `partition_definitions` with `target_kind='op'` and `target_key=<step_key>`
  - `partition_keys` with deterministic `sort_index`
- Backfills are launched per job, using the shared partition domain for that job.

## Define Partitions in Code

Attach partitions directly to an op:

```go
extract := dag.Op[ops.ExtractInput, ops.ExtractOutput]("extract", myOps.Extract).
	WithPartition(
		dag.DailyFrom(
			time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			dag.WithTimezone("UTC"),
		),
		"raw_articles",
	)
```

Typed helpers:

- `dag.StringPartitions(...)`
- `dag.MinutelyEvery(intervalMinutes, start, opts...)`
- `dag.HourlyFrom(start, opts...)`
- `dag.DailyFrom(start, opts...)`
- `dag.WeeklyFrom(start, opts...)`
- `dag.MonthlyFrom(start, opts...)`
- `dag.MultiPartitions(dag.Dimension(...), dag.Dimension(...))`

Time options:

- `dag.WithTimezone("UTC")`
- `dag.WithLocation(loc)`
- `dag.WithEndOffset(n)`
- `dag.WithFormat(layout)`

## Custom Partitions

For fully custom behavior, implement `CustomPartition`:

```go
type RegionPartition struct{}

func (RegionPartition) Spec() dag.CustomPartitionSpec {
	return dag.CustomPartitionSpec{
		Kind:    dag.PartitionDefinitionStatic,
		Version: "v1",
		Assets:  []string{"raw_articles"},
		Config:  map[string]any{"group": "regions"},
	}
}

func (RegionPartition) Keys(ctx context.Context, now time.Time) ([]string, error) {
	return []string{"us", "eu"}, nil
}

extract := dag.Op[ops.ExtractInput, ops.ExtractOutput]("extract", myOps.Extract).
	WithCustomPartition(RegionPartition{})
```

## Runtime Partition Context in Ops

When a run is partition-targeted, op code can read typed partition metadata directly from `context.Context`:

```go
func (o *Ops) Extract(ctx context.Context, in ExtractInput) (ExtractOutput, error) {
	if partition, ok := dag.RunPartitionMetaFromContext(ctx); ok {
		slog.Info("partition run",
			"mode", partition.SelectionMode,
			"keys", partition.Keys,
			"backfill_key", partition.BackfillKey,
		)
	}
	return ExtractOutput{}, nil
}
```

`RunPartitionMeta` includes:

- `DefinitionID`
- `SelectionMode`
- `Keys`
- `Ranges`
- `BackfillKey`

## Startup Sync Behavior

No manual SQL bootstrap is required for standard usage.

When `daggo.Run(...)` or `daggo.Open(...)` initializes runtime deps, registry sync:

1. Upserts job/node/edge metadata.
2. Upserts op partition definitions (`target_kind='op'`, `target_key=<step_key>`).
3. Rebuilds partition keys for each partitioned op.
4. Removes stale op partition definitions for ops no longer partitioned.

## Backfill UI and RPC

For partitioned jobs, open `Jobs -> <job> -> Overview`.

Launch supports:

- Selection modes:
  - `all`
  - `single`
  - `range`
  - `subset`
- Policy modes:
  - `multi_run` (with `max partitions/run`)
  - `single_run`

Routes:

- `/rpc/backfills/backfill-launch`
- `/rpc/backfills/backfills-get-many`
- `/rpc/backfills/backfill-by-key`

Backfill state reconciliation is automatic once runs finish:

- successful run -> linked backfill partitions become `materialized`
- failed/canceled run -> linked backfill partitions become `failed_downstream`
- backfill aggregate counts/status update automatically (`running`, `completed`, `failed`)

### Launch Example

```bash
curl -sS -X POST http://localhost:8000/rpc/backfills/backfill-launch \
  -H 'Content-Type: application/json' \
  -d '{
    "job_key": "content_ingestion",
    "selection_mode": "range",
    "range_start_partition": "2026-03-01",
    "range_end_partition": "2026-03-10",
    "policy_mode": "multi_run",
    "max_partitions_per_run": 2,
    "triggered_by": "cli"
  }'
```

## Selection and Policy Semantics

Selection modes:

- `all`: every key in `partition_keys` for the resolved definition.
- `single`: one key.
- `range`: inclusive range by key order.
- `subset`: explicit key list.

Policy modes:

- `single_run`: one run targeting all selected keys.
- `multi_run`: chunk selection into runs of `max_partitions_per_run`.

## Current Guardrails

Backfill launch validates:

- partition definition key count (`<= 100000`)
- selection size (`<= 100000`)
- planned run count (`<= 5000`)
- `max_partitions_per_run` (`<= 1000`)

## Current Limitation

Within a single job, partitioned ops must resolve to one shared partition domain (same definition and keyspace). Mixed partition domains in one job are rejected during startup sync in this experimental release.

## Troubleshooting

`job has no partition definition`

- No op in the job is currently configured with `WithPartition(...)` or `WithCustomPartition(...)`.
- Legacy job-level metadata can still be read, but op-level declarations are the canonical path.

`partition definition has no partition keys`

- The resolved partition definition produced zero keys.
- Verify start/cadence options and custom `Keys(...)` behavior.

`unsupported selection_mode` or `unsupported policy_mode`

- Use supported values from this guide exactly.

## Related Docs

- [USAGE_GUIDE.md](./USAGE_GUIDE.md)
- [COMING_FROM_DAGSTER.md](./COMING_FROM_DAGSTER.md)
