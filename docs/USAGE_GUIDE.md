# DAGGO Usage Guide

This guide covers day-to-day usage of DAGGO as an operator and as a developer.

## Operator Workflow

1. Start DAGGO:
```bash
go run ./cmd/api/main.go
```
2. Open the UI at `http://localhost:8000/`.
3. Use left navigation:
- `Overview`: time-based execution timeline (running, succeeded, failed, scheduled).
- `Jobs`: structural inventory of job definitions and schedules.
- `Runs`: run-centric history with filters.
4. Launch a run from a job detail page.
5. Open run detail to inspect:
- step DAG
- step state counts
- event feed (search/filter)
- rerun from failed step
- terminate run (when running)

## Developer Workflow

## 1. Define Steps

Steps are strongly typed Go functions:

```go
type ExtractInput struct{}
type ExtractOutput struct {
	Records []string
}

func RunExtract(ctx context.Context, in ExtractInput) (ExtractOutput, error) {
	return ExtractOutput{Records: []string{"a", "b"}}, nil
}
```

Use `dag.Define` to register each step.

## 2. Compose a Job

Use `dag.NewJob(...).Add(...).MustBuild()`:

```go
extract := dag.Define[ExtractInput, ExtractOutput]("extract", RunExtract)

job := dag.NewJob("example_job").
	WithDisplayName("Example Job").
	WithDescription("Example DAGGO job").
	Add(extract).
	AddSchedule(dag.ScheduleDefinition{
		Key:      "hourly",
		CronExpr: "0 * * * *",
		Timezone: "UTC",
		Enabled:  true,
	}).
	MustBuild()
```

## 3. Register Jobs

Add built jobs in `jobs/registry.go` via `registry.MustRegister(...)`.

## 4. Generate and Run

```bash
make gen-all
go run ./cmd/api/main.go
```

## Input Resolution Semantics

DAGGO wiring is type-driven and validated at build time (`MustBuild()`):

- Singular input `T`: exactly one upstream producer of `T` is required.
- Slice input `[]T`: all upstream producers of `T` are injected in deterministic topological order; zero is allowed.
- Pointer input `*T`: zero or one producer; zero injects `nil`, more than one is an error.

No string-based wiring. No implicit preference rules.

## Scheduling

- Cron-based schedules are attached to jobs.
- Scheduler runs on a fixed tick (`SCHEDULER_TICK_SECONDS`).
- Non-backfilling by default: it does not enqueue historical missed windows.

## Execution Model

Runs are created via RPC and executed asynchronously:

- Default mode: `subprocess` (detached worker process per run)
- Optional mode: `in_process`

Concurrency knobs:

- `RUN_MAX_CONCURRENT_RUNS`
- `RUN_MAX_CONCURRENT_STEPS`

## Observability

DAGGO stores run, step, and event state in SQLite:

- Overview timeline: current and historical activity plus scheduled future intervals.
- Runs list: dense operational run table.
- Run detail: DAG visualization + step/event diagnostics.

## Safe Deploy/Drain

Create `runtime/WILL_DEPLOY` to trigger drain behavior:

- New run creation/schedule enqueue is blocked.
- Existing active runs are allowed to finish until grace timeout.
- Process exits once idle (or after forced timeout).

See `docs/WILL_DEPLOY_DRAIN_LOCK_PLAN.md`.

## Troubleshooting

- UI shows stale generated client:
  - Run `make gen-sdk` and `make gen-web`.
- SQL/query mismatch:
  - Run `make gen`.
- Broken local state:
  - Remove `daggo.sqlite*` and restart.

## Monorepo Direction

Current repository combines engine + implementation.

Planned direction:
- extract DAGGO engine/runtime into a standalone package
- keep project-specific job implementations in separate repos/services
