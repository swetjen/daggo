# DAGGO Usage Guide

This guide covers the package-level runtime flow for importing DAGGO into another Go application.

## Package Flow

1. Define typed steps with `dag.Op[I, O]`.
2. Build a job with `dag.NewJob(...).Add(...).MustBuild()`.
3. Start from `daggo.DefaultConfig()`.
4. Launch DAGGO with `daggo.Main(...)`, `daggo.Run(...)`, or build an `app` with `daggo.NewApp(...)`.

## Minimal Startup

```go
cfg := daggo.DefaultConfig()
cfg.Admin.Port = "8080"
cfg.DisableUI = false
cfg.Database.SQLite.Path = "runtime/daggo.sqlite"

if err := daggo.Main(context.Background(), cfg, daggo.WithJobs(job)); err != nil {
	log.Fatal(err)
}
```

`daggo.Main(...)` automatically:

- opens the configured database
- applies bundled migrations
- syncs registered jobs into the metadata tables
- serves the embedded admin UI
- serves RPC docs under `/rpc/docs/`
- manages DAGGO's internal worker subprocess command

Current schedules are taken from the jobs registered in memory at startup. DAGGO persists scheduler runtime state and run history, not future schedule definitions.

If you do not want the UI exposed, set `cfg.DisableUI = true`. DAGGO will still serve `/rpc/` and `/rpc/docs/`.

## Recommended Project Layout

For a real imported application, prefer separating graph wiring from operational code:

```text
myapp/
  main.go
  jobs/
    content_ingestion.go
    customer_sync.go
  ops/
    content_ops.go
    customer_ops.go
  resources/
    deps.go
```

Recommended split:

- `jobs/`: DAGGO job definitions, schedules, and graph composition.
- `ops/`: concrete step implementations.
- `resources/`: shared clients, repositories, and external service handles.

This keeps job definitions declarative and makes dependency handling straightforward.

### Dependency Pattern

Prefer passing dependencies into a struct and exposing step functions as methods on that struct instead of relying on package globals.

```go
package resources

import (
	"log/slog"
	"net/http"

	"github.com/swetjen/daggo/resources/ollama"
	playwrightresource "github.com/swetjen/daggo/resources/playwright"
	"github.com/swetjen/daggo/resources/s3resource"
)

type Deps struct {
	Logger *slog.Logger
	HTTP   *http.Client
	CRUD   any

	Playwright *playwrightresource.RemoteResource
	S3         *s3resource.Resource
	Gemini     *http.Client
	OpenAPI    *http.Client
	Ollama     *ollama.Resource
}
```

Then mount those dependencies onto an ops struct:

```go
package ops

import (
	"context"

	"github.com/swetjen/daggo/dag"
	"myapp/resources"
)

type MyOps struct {
	deps resources.Deps
}

func NewMyOps(deps resources.Deps) *MyOps {
	return &MyOps{deps: deps}
}

func (o *MyOps) ScrapePageOp(ctx context.Context, in dag.NoInput) (ScrapePageOutput, error) {
	_ = ctx
	_ = in
	o.deps.Logger.Info("scraping page")
	return ScrapePageOutput{}, nil
}
```

Then bind those methods in `jobs/`:

```go
package jobs

import (
	"github.com/swetjen/daggo/dag"
	"myapp/ops"
)

func ContentIngestion(myOps *ops.MyOps) dag.JobDefinition {
	scrape := dag.Op[dag.NoInput, ops.ScrapePageOutput]("scrape_page", myOps.ScrapePageOp)
	extract := dag.Op[ops.ExtractInput, ops.ExtractOutput]("extract", myOps.ExtractOp)
	update := dag.Op[ops.UpsertInput, ops.UpsertOutput]("update", myOps.UpsertOp)

	return dag.NewJob("content_ingestion").
		Add(scrape, extract, update).
		AddSchedule(dag.ScheduleDefinition{
			CronExpr: "*/15 * * * *",
			Timezone: "UTC",
			Enabled:  true,
		}).
		MustBuild()
}
```

This pattern gives you:

- explicit dependency injection
- simpler unit testing for ops
- cleaner separation between orchestration shape and operational code
- readable ops that follow a standard Go pattern: input type, output type, named `...Op` function

## Embedded App Mode

If you want to mount DAGGO inside a larger HTTP server, create an app directly:

```go
app, err := daggo.NewApp(context.Background(), cfg, daggo.WithJobs(job))
if err != nil {
	log.Fatal(err)
}
defer app.Close()

myMux := http.NewServeMux()
myMux.Handle("/daggo/", http.StripPrefix("/daggo", app.Handler()))
```

## Configuration Surface

The public config is centered on `daggo.Config`:

- `Admin.Port`
- `DisableUI`
- `Database.Driver`
- `Database.SQLite.Path`
- `Database.SQLite.DSN`
- `Database.Postgres.*`
- `Execution.QueueSize`
- `Execution.Mode`
- `Execution.MaxConcurrentRuns`
- `Execution.MaxConcurrentSteps`
- `Scheduler.Enabled`
- `Scheduler.Key`
- `Scheduler.TickSeconds`
- `Scheduler.MaxDuePerTick`
- `Deploy.LockPath`
- `Deploy.PollSeconds`
- `Deploy.DrainGraceSeconds`

## Database Modes

SQLite is the default and the recommended way to get started.

```go
cfg := daggo.DefaultConfig()
cfg.Database.SQLite.Path = "runtime/daggo.sqlite"
```

PostgreSQL is supported as an explicit opt-in mode.

```go
cfg := daggo.DefaultConfig()
cfg.Database.Driver = daggo.DatabaseDriverPostgres
cfg.Database.Postgres.Host = "db.internal"
cfg.Database.Postgres.Port = 5432
cfg.Database.Postgres.User = "daggo"
cfg.Database.Postgres.Password = "secret"
cfg.Database.Postgres.Database = "platform"
cfg.Database.Postgres.Schema = "customer_a_daggo"
cfg.Database.Postgres.SSLMode = "require"
```

Important behavior:

- SQLite remains the default unless the driver is explicitly set to `postgres`.
- DAGGO provisions into the configured PostgreSQL schema, not the whole database.
- Startup creates the schema if needed and runs bundled up-migrations automatically.

Environment example:

```bash
export DAGGO_DATABASE_DRIVER=postgres
export DAGGO_POSTGRES_HOST=db.internal
export DAGGO_POSTGRES_PORT=5432
export DAGGO_POSTGRES_USER=daggo
export DAGGO_POSTGRES_PASSWORD=secret
export DAGGO_POSTGRES_DATABASE=platform
export DAGGO_POSTGRES_SCHEMA=customer_a_daggo
export DAGGO_POSTGRES_SSLMODE=require
```

More detail lives in [docs/POSTGRES_RUNTIME_SPEC.md](POSTGRES_RUNTIME_SPEC.md).

## Input Resolution

DAGGO wiring is type-driven and validated at build time:

- Singular input `T`: exactly one upstream producer of `T`.
- Slice input `[]T`: all upstream producers of `T`, in deterministic topological order.
- Pointer input `*T`: zero or one producer, with `nil` when absent.

## Schedules

`dag.ScheduleDefinition.Key` is optional. If you omit it, DAGGO derives a stable key from the cron expression, such as `every_minute`, `every_15_minutes`, or `hourly`.

Important runtime behavior:

- Current schedules are registry-backed, not loaded from persisted schedule rows.
- DAGGO persists scheduler bookkeeping by `(job_key, schedule_key)` for dedupe and next-run tracking.
- Removing a schedule from code clears that bookkeeping but preserves historical `runs`.

No string-based wiring is required.

## Execution

Runs are created through the RPC API and executed asynchronously.

- Default mode: `subprocess`
- Optional mode: `in_process`

Concurrency controls:

- `cfg.Execution.MaxConcurrentRuns`
- `cfg.Execution.MaxConcurrentSteps`

## Local Repo Commands

- `make gen`
- `make gen-sdk`
- `make gen-web`
- `make gen-all`
- `go test ./...`
