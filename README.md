# DAGGO

<p align="center">
  <img src="frontend-web/src/assets/daggo.png" alt="DAGGO logo" width="220" />
</p>

[![Build](https://github.com/swetjen/daggo/actions/workflows/ci.yml/badge.svg)](https://github.com/swetjen/daggo/actions/workflows/ci.yml)
[![Version](https://img.shields.io/github/v/release/swetjen/daggo?sort=semver)](https://github.com/swetjen/daggo/releases)

DAGGO is a Go-native workflow orchestrator for teams that want to define jobs in Go, ship a single binary, and still get an admin UI, scheduling, run history, and step-level diagnostics out of the box.

## What You Get

- Typed DAG authoring with build-time validation.
- SQLite by default, with migrations applied automatically at startup.
- Embedded web admin UI served by the same Go process.
- RPC API plus generated client support.
- Background scheduling and async run execution.
- Internal worker bootstrapping handled by DAGGO, so importing apps do not need to implement private subprocess commands.

## Install

```bash
go get github.com/swetjen/daggo
```

## Quick Start

```go
package main

import (
	"context"
	"log"
	"strings"

	"github.com/swetjen/daggo"
	"github.com/swetjen/daggo/dag"
)

type ScrapePageOutput struct {
	URL  string
	HTML string
}

type ExtractTitleInput struct {
	Page ScrapePageOutput
}

type ExtractTitleOutput struct {
	Title string
}

type ExtractEntitiesInput struct {
	Page ScrapePageOutput
}

type ExtractEntitiesOutput struct {
	Entities []string
}

type ExtractLinksInput struct {
	Page ScrapePageOutput
}

type ExtractLinksOutput struct {
	Links []string
}

type UpdateIndexInput struct {
	Title    ExtractTitleOutput
	Entities ExtractEntitiesOutput
	Links    ExtractLinksOutput
}

type UpdateIndexOutput struct {
	RecordsUpserted int
}

func main() {
	cfg := daggo.DefaultConfig()
	cfg.Admin.Port = "8080"
	cfg.Database.SQLite.Path = "runtime/daggo.sqlite"

	if err := daggo.Main(context.Background(), cfg, daggo.WithJobs(buildContentIngestionJob())); err != nil {
		log.Fatal(err)
	}
}

func buildContentIngestionJob() dag.JobDefinition {
	scrapePage := dag.Define[dag.NoInput, ScrapePageOutput]("scrape_page", func(_ context.Context, _ dag.NoInput) (ScrapePageOutput, error) {
		return ScrapePageOutput{
			URL:  "https://example.com/blog/daggo",
			HTML: "<html><head><title>DAGGO</title></head><body>Daggo extracts entities and links.</body></html>",
		}, nil
	})

	extractTitle := dag.Define[ExtractTitleInput, ExtractTitleOutput]("extract_title", func(_ context.Context, in ExtractTitleInput) (ExtractTitleOutput, error) {
		title := "Untitled"
		if strings.Contains(in.Page.HTML, "<title>DAGGO</title>") {
			title = "DAGGO"
		}
		return ExtractTitleOutput{Title: title}, nil
	})

	extractEntities := dag.Define[ExtractEntitiesInput, ExtractEntitiesOutput]("extract_entities", func(_ context.Context, _ ExtractEntitiesInput) (ExtractEntitiesOutput, error) {
		return ExtractEntitiesOutput{Entities: []string{"Daggo", "SQLite", "Workflow"}}, nil
	})

	extractLinks := dag.Define[ExtractLinksInput, ExtractLinksOutput]("extract_links", func(_ context.Context, in ExtractLinksInput) (ExtractLinksOutput, error) {
		return ExtractLinksOutput{Links: []string{in.Page.URL, "https://example.com/docs/daggo"}}, nil
	})

	updateIndex := dag.Define[UpdateIndexInput, UpdateIndexOutput]("update_search_index", func(_ context.Context, _ UpdateIndexInput) (UpdateIndexOutput, error) {
		return UpdateIndexOutput{RecordsUpserted: 1}, nil
	})

	return dag.NewJob("content_ingestion").
		WithDisplayName("Content Ingestion").
		WithDescription("Scrape a page, fan out extraction work, then persist a consolidated record.").
		Add(scrapePage, extractTitle, extractEntities, extractLinks, updateIndex).
		AddSchedule(dag.ScheduleDefinition{
			Key:      "every_15_minutes",
			CronExpr: "*/15 * * * *",
			Timezone: "UTC",
			Enabled:  true,
		}).
		MustBuild()
}
```

```text
scrape_page
  -> extract_title ----\
  -> extract_entities --+-> update_search_index
  -> extract_links ----/
```

See [examples/content_ingestion/main.go](examples/content_ingestion/main.go) for the full example.

## Startup Flow

Calling `daggo.Main(...)` or `daggo.Run(...)` gives new users a working runtime immediately:

1. Opens the configured database.
2. Applies bundled migrations automatically.
3. Syncs registered jobs into DAGGO metadata tables.
4. Serves the embedded admin UI and RPC docs.
5. Starts the scheduler and async executor.
6. Handles DAGGO's internal worker subprocess command inside the same application binary.

## Configuration

Start from `daggo.DefaultConfig()` and override what you need:

```go
cfg := daggo.DefaultConfig()
cfg.Admin.Port = "8080"
cfg.Database.SQLite.Path = "runtime/daggo.sqlite"
```

Available config areas:

- `cfg.Admin.Port`: web admin / RPC listen port.
- `cfg.Database`: database driver and connection settings.
- `cfg.Execution`: queue size, execution mode, run concurrency, step concurrency.
- `cfg.Scheduler`: scheduler enablement and tick controls.
- `cfg.Deploy`: deploy-drain lock settings.

Environment-based startup is still available through `daggo.LoadConfigFromEnv()` for the repo's sample binary and local development.

### Database Modes

SQLite is the default and is fully implemented today.

```go
cfg := daggo.DefaultConfig()
cfg.Database.SQLite.Path = "runtime/daggo.sqlite"
```

PostgreSQL is now part of the public config surface, but runtime support is intentionally not implemented yet.

```go
cfg := daggo.DefaultConfig()
cfg.Database.Driver = daggo.DatabaseDriverPostgres
cfg.Database.Postgres.Host = "db.internal"
cfg.Database.Postgres.Port = 5432
cfg.Database.Postgres.User = "daggo"
cfg.Database.Postgres.Password = "secret"
cfg.Database.Postgres.Database = "platform"
cfg.Database.Postgres.Schema = "customer_a_daggo"
```

The planned provisioning and migration model is documented in [docs/POSTGRES_RUNTIME_SPEC.md](docs/POSTGRES_RUNTIME_SPEC.md).

## Local Development

1. Install prerequisites: Go `1.25+`, `bun`, `make`.
2. Copy the env template:

```bash
cp .env.example .env
```

3. Generate SQL, SDK, and frontend assets:

```bash
make gen-all
```

4. Start the sample binary:

```bash
go run ./cmd/api
```

5. Open:

- UI: `http://localhost:8000/`
- RPC docs: `http://localhost:8000/rpc/docs/`
- OpenAPI: `http://localhost:8000/rpc/openapi.json`

## Key Commands

- `make gen`: regenerate sqlc output.
- `make gen-sdk`: regenerate RPC clients.
- `make gen-web`: rebuild frontend assets.
- `make gen-all`: run every generation step.
- `go test ./...`: run the Go test suite.

## Documentation

- Usage guide: [docs/USAGE_GUIDE.md](docs/USAGE_GUIDE.md)
- Coming from Dagster: [docs/COMING_FROM_DAGSTER.md](docs/COMING_FROM_DAGSTER.md)
- PostgreSQL runtime plan: [docs/POSTGRES_RUNTIME_SPEC.md](docs/POSTGRES_RUNTIME_SPEC.md)
- Deploy-drain behavior: [docs/WILL_DEPLOY_DRAIN_LOCK_PLAN.md](docs/WILL_DEPLOY_DRAIN_LOCK_PLAN.md)
