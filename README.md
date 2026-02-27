# DAGGO

[![Build](https://github.com/swetjen/daggo/actions/workflows/ci.yml/badge.svg)](https://github.com/swetjen/daggo/actions/workflows/ci.yml)
[![Version](https://img.shields.io/github/v/release/swetjen/daggo?sort=semver)](https://github.com/swetjen/daggo/releases)

DAGGO is a Go-native workflow orchestrator focused on typed DAG authoring and operator-grade run visibility.

Current release line: `v0.10.x`.

## What DAGGO Provides

- Strongly typed step contracts with deterministic dependency resolution.
- Build-time DAG validation for missing and ambiguous providers.
- Integrated scheduler with cron + timezone support.
- Async run execution with step-level concurrency controls.
- Embedded UI for overview, jobs, runs, and deep run diagnostics.
- RPC API for launching runs, rerunning steps, querying runs/events, and schedule visibility.
- Single binary runtime with deploy-drain lock behavior.

## Architecture Snapshot

- `dag/`: graph contracts, validation, scheduler, executor.
- `jobs/`: job registration and example/test jobs.
- `handlers/`: Virtuous RPC handlers.
- `db/`: sqlite schema + queries + generated sqlc code.
- `frontend-web/`: React UI embedded into the Go binary.
- `cmd/api/`: executable entrypoint (`server` + `worker` modes).

## Quick Start

1. Install prerequisites: Go `1.25+`, `bun`, `make`.
2. Copy the env template:
```bash
cp .env.example .env
```
3. Install local dev tooling:
```bash
make deps
```
4. Generate SQL/RPC/web outputs:
```bash
make gen-all
```
5. Start DAGGO:
```bash
go run ./cmd/api/main.go
```
6. Open:
- UI: `http://localhost:8000/`
- RPC docs: `http://localhost:8000/rpc/docs/`
- OpenAPI: `http://localhost:8000/rpc/openapi.json`

## Usage Examples

## Define a Typed Step

```go
type FetchInput struct{}
type FetchOutput struct {
	Records []string
}

func RunFetch(ctx context.Context, in FetchInput) (FetchOutput, error) {
	return FetchOutput{Records: []string{"a", "b"}}, nil
}
```

## Compose a Job

```go
fetch := dag.Define[FetchInput, FetchOutput]("fetch", RunFetch)

job := dag.NewJob("example_job").
	WithDisplayName("Example Job").
	Add(fetch).
	AddSchedule(dag.ScheduleDefinition{
		Key:      "hourly",
		CronExpr: "0 * * * *",
		Timezone: "UTC",
		Enabled:  true,
	}).
	MustBuild()
```

## Run a Single Worker Process

```bash
go run ./cmd/api/main.go worker --run-id 123
```

## Reset Local Runtime DB

```bash
rm -f daggo.sqlite daggo.sqlite-shm daggo.sqlite-wal daggo.sqlite-journal
```

## Key Commands

- `make gen`: regenerate sqlc output
- `make gen-sdk`: regenerate RPC clients
- `make gen-web`: build frontend assets
- `make gen-all`: run all generation steps
- `make test`: run Go tests
- `make run`: run with autoreload

## Environment

All supported environment variables are documented in `.env.example`.

## Releases

Tagged releases publish prebuilt CLI binaries for:
- Linux (`amd64`, `arm64`)
- macOS (`amd64`, `arm64`)
- Windows (`amd64`, `arm64`)

Assets are attached automatically on the GitHub Releases page.

## Core RPC Surface

- Jobs: `jobs.JobsGetMany`, `jobs.JobByKey`
- Runs: `runs.RunCreate`, `runs.RunRerunStepCreate`, `runs.RunsGetMany`, `runs.RunByID`, `runs.RunEventsGetMany`, `runs.RunTerminate`
- Schedules: `schedules.SchedulesGetMany`

## Documentation

- Usage guide: `docs/USAGE_GUIDE.md`
- Coming from Dagster: `docs/COMING_FROM_DAGSTER.md`
- Deployment lock model: `docs/WILL_DEPLOY_DRAIN_LOCK_PLAN.md`
- Daemon split plan: `docs/EXECUTION_DAEMON_SPLIT_PLAN.md`
