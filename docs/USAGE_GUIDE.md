# DAGGO Usage Guide

This guide covers the package-level runtime flow for importing DAGGO into another Go application.

## Package Flow

1. Define typed steps with `dag.Op[I, O]`.
2. Build a job with `dag.NewJob(...).Add(...).MustBuild()`.
3. Optionally build queues with `daggo.NewQueue[T](...)`.
4. Start from `daggo.DefaultConfig()`.
5. Launch DAGGO with `daggo.Run(...)` for jobs only, or `daggo.RunDefinitions(...)` / `daggo.OpenDefinitions(...)` when queues are involved.

## Minimal Startup

```go
cfg := daggo.DefaultConfig()
cfg.Admin.Port = "8080"
cfg.Admin.SecretKey = "replace-me"
cfg.DisableUI = false
cfg.Database.SQLite.Path = "/tmp/daggo.sqlite"

if err := daggo.Run(context.Background(), cfg, job); err != nil {
	log.Fatal(err)
}
```

`daggo.Run(...)` automatically:

- opens the configured database
- applies bundled migrations automatically
- syncs registered jobs into the metadata tables
- serves the embedded admin UI
- serves RPC docs under `/rpc/docs/`
- manages DAGGO's internal worker subprocess command

Current schedules are taken from the jobs registered in memory at startup. DAGGO persists scheduler runtime state and run history, not future schedule definitions.

If you do not want the UI exposed, set `cfg.DisableUI = true`. DAGGO will still serve `/rpc/` and `/rpc/docs/`.

## Queues

Queues are the canonical way to let DAGGO orchestrate external work without taking ownership of your underlying queue storage.

A queue definition includes:

- one or more attached jobs
- a loader that emits normalized queue items into DAGGO
- a partition resolver for each queue item
- an optional route, usually implemented with Virtuous, that stages work into your own queue store

```go
type ImportEnvelope struct {
	CustomerID string `json:"customer_id"`
	BatchID    string `json:"batch_id"`
}

importQueue := daggo.NewQueue[ImportEnvelope]("customer_imports").
	WithRoute("/queues/customer-imports", myVirtuousHandler).
	WithLoader(loadQueuedImports, daggo.QueueLoaderOptions{
		Mode:      daggo.QueueLoadModePoll,
		PollEvery: 2 * time.Second,
	}).
	WithPartitionKey(func(item ImportEnvelope) (string, error) {
		return item.CustomerID, nil
	}).
	AddJobs(importJob, auditJob).
	MustBuild()

if err := daggo.RunDefinitions(context.Background(), cfg, importQueue); err != nil {
	log.Fatal(err)
}
```

Queue-attached jobs are unordered fan-out peers. For each accepted queue item, DAGGO creates one run per attached job, persists queue item state, and exposes queue items plus partitions in the admin UI.

Use these helpers in step code when the current run came from a queue:

- `daggo.QueueMetaFromContext(ctx)`
- `daggo.QueuePayloadFromContext[T](ctx)`

Successful step outputs may publish metadata into the queue item detail page by implementing `daggo.MetadataProvider`.

## Runner Model

By default, DAGGO runs each job execution in `subprocess` mode. When a run starts, DAGGO launches a separate worker PID from the same binary and that worker owns the run lifecycle.

Relevant execution settings:

- `cfg.Execution.Mode`
- `cfg.Execution.MaxConcurrentRuns`
- `cfg.Execution.MaxConcurrentSteps`

Because runs execute in a separate worker process, the web server can restart independently of the active runner instead of tying run execution to a request-serving goroutine. DAGGO’s deploy-drain support is intended to let new code roll out without immediately disrupting active workers. Additional daemon and runner configurations are planned.

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
    crud.go
    openai.go
    playwright.go
    gemini.go
    s3.go
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
	"context"

	"github.com/openai/openai-go/v3"
	"myapp/db"
	"github.com/swetjen/daggo/resources/ollama"
	playwrightresource "github.com/swetjen/daggo/resources/playwright"
	"github.com/swetjen/daggo/resources/s3resource"
	"google.golang.org/genai"
)

type ScrapeResult struct {
	Body       string
	StatusCode int
}

type Deps struct {
	CRUD *db.Queries

	Playwright *playwrightresource.RemoteResource
	S3         *s3resource.Resource
	Gemini     *genai.Client
	OpenAI     *openai.Client
	Ollama     *ollama.Resource

	Scraper func(ctx context.Context, targetURL string) (ScrapeResult, error)
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

func (o *MyOps) ScrapePageOp(ctx context.Context, input dag.NoInput) (ScrapePageOutput, error) {
	if err := ctx.Err(); err != nil {
		return ScrapePageOutput{}, err
	}

	// Run some processing on the input.
	result, err := o.deps.Scraper(ctx, o.scrapeTargetURL(input))
	if err != nil {
		return ScrapePageOutput{}, err
	}

	return ScrapePageOutput{
		Body:       result.Body,
		StatusCode: result.StatusCode,
	}, nil
}

func (o *MyOps) scrapeTargetURL(dag.NoInput) string {
	return "https://example.com"
}

type ScrapePageOutput struct {
	Body       string
	StatusCode int
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

If you want to mount DAGGO inside a larger HTTP server, open an app directly:

```go
app, err := daggo.Open(context.Background(), cfg, job)
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
- `Admin.SecretKey`
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
- `Retention.RunDays`

## Database Modes

SQLite is the default and the recommended way to get started.

```go
cfg := daggo.DefaultConfig()
cfg.Database.SQLite.Path = "/tmp/daggo.sqlite"
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
cfg.Database.Postgres.Schema = "my_project"
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
export DAGGO_POSTGRES_SCHEMA=my_project
export DAGGO_POSTGRES_SSLMODE=require
```

More detail lives in [docs/POSTGRES_RUNTIME_SPEC.md](POSTGRES_RUNTIME_SPEC.md).

## RPC Route Guard

If you want to protect DAGGO’s RPC surface, configure a bearer secret:

```go
cfg := daggo.DefaultConfig()
cfg.Admin.SecretKey = "replace-me"
cfg.DisableUI = true
```

When set, DAGGO requires `Authorization: Bearer <secret>` for `/rpc/` and `/rpc/docs/`.

Generated clients pass the same value through their auth option:

```ts
const client = createClient("http://localhost:8000")
await client.jobs.JobsGetMany({ limit: 50, offset: 0 }, { auth: "replace-me" })
```

The embedded UI is not authenticated yet, so `cfg.DisableUI = true` is the secure deployment mode today.

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
