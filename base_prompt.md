# DAGGO Sprint Plan (10-12 sprints)

## Global principles
- Each sprint enables the next.
- Each sprint produces a working artifact.
- Each sprint has explicit acceptance criteria.
- Keep scope tight (days, not weeks).

---

## Sprint 0: Project Bootstrap & Ground Rules
**Goal**: Establish a buildable, opinionated baseline repo aligned with Virtuous BYODB.

**Backend**
- Fork/scaffold from `virtuous/example/byodb`.
- Rename + rebrand to DAGGO.
- Confirm:
  - `cmd/api/main.go`
  - Router assembly
  - Embedded FS
  - `sqlc` + Goose wired
- Add empty domain packages:
  - `nodes`
  - `jobs`
  - `runs`
  - `events`
  - `executor`

**Frontend**
- Minimal React app scaffold.
- Served via embedded FS.
- "DAGGO" splash page only.

**Infra**
- Local Postgres (docker-compose OK but optional).
- `make dev`, `make gen`, `make migrate`.

**Acceptance criteria**
- `go build ./...` passes.
- DB migrates cleanly.
- Visiting `/` serves the React app.
- No DAGGO logic yet.

---

## Sprint 1: Core Domain Model (No Execution)
**Goal**: Define DAGGO's conceptual spine in Go.

**Backend**
- Define core types:
  - `Node`
  - `NodeInput`
  - `NodeOutput`
  - `Job`
  - `Graph`
- Enforce:
  - Typed inputs/outputs
  - Context-required signatures
- No execution yet.
- No DB yet.

**Frontend**
- None (stub only).

**Infra**
- None.

**Acceptance criteria**
- Nodes + Jobs compile.
- Example Job definition compiles.
- No runtime behavior.

---

## Sprint 2: Node Registration & Graph Validation
**Goal**: Fail fast, like `http.ServeMux`.

**Backend**
- Central registry:
  - `daggo.RegisterNode`
  - `daggo.RegisterJob`
- Dependency resolution:
  - Type-based inference
  - Explicit wiring overrides
- Validation:
  - Cycles
  - Missing deps
  - Ambiguous outputs
  - Panic or structured error at startup

**Frontend**
- None.

**Infra**
- None.

**Acceptance criteria**
- Invalid graphs crash at startup.
- Valid graphs register cleanly.
- Clear error messages.

---

## Sprint 3: Persistence: Event Log & Core Tables
**Goal**: Durable truth before execution.

**Backend**
- Goose migrations for:
  - `jobs`
  - `nodes`
  - `runs`
  - `run_events`
- `sqlc` queries:
  - Create run
  - Append event
  - Query run state
- Event-first model.

**Frontend**
- None.

**Infra**
- Migration embedding verified.

**Acceptance criteria**
- DB schema migrates.
- Events can be written + queried.
- No executor yet.

---

## Sprint 4: Minimal Executor (In-Process)
**Goal**: Execute a Job end-to-end.

**Backend**
- In-process executor:
  - Goroutines
  - Dependency ordering
  - Context propagation
- Emit events:
  - `RunStarted`
  - `NodeStarted`
  - `NodeSucceeded`
  - `NodeFailed`
- Capture panics as failures.

**Frontend**
- None.

**Infra**
- None.

**Acceptance criteria**
- Simple Job executes.
- Events persisted.
- Failures recorded.

---

## Sprint 5: Typed RPC API (Virtuous)
**Goal**: Expose DAGGO via typed RPC only.

**Backend**
- RPC handlers:
  - `JobsList`
  - `JobGet`
  - `RunCreate`
  - `RunGet`
  - `RunEvents`
- Use Virtuous exclusively.
- Codegen TS client.

**Frontend**
- Client compiles.
- Can call `JobsList`.

**Infra**
- None.

**Acceptance criteria**
- No OpenAPI handlers.
- RPC client works.
- API returns real DB data.

---

## Sprint 6: Frontend: Job & Graph Views
**Goal**: Make DAGGO visible.

**Frontend**
- Job list page.
- Job detail page:
  - Graph visualization (static SVG/Canvas OK).
- Fetch via RPC client.

**Backend**
- Graph DTOs if needed.

**Acceptance criteria**
- Jobs visible in UI.
- Graph renders correctly.
- No execution UI yet.

---

## Sprint 7: Run Lifecycle UI
**Goal**: Observe execution.

**Frontend**
- Run list per Job.
- Run detail:
  - Timeline
  - Node status
  - Logs/events

**Backend**
- Efficient run/event queries.

**Acceptance criteria**
- Can start a run.
- Can watch it complete/fail.
- Logs visible.

---

## Sprint 8: Inputs, Parameters & Partitions (v1)
**Goal**: Make runs configurable.

**Backend**
- Typed run inputs.
- Partition abstraction (first-class, minimal).
- Persist parameters.

**Frontend**
- Run dialog with inputs.
- Partition selector (simple).

**Acceptance criteria**
- Parameterized runs work.
- Partitions stored.
- No backfill UI yet.

---

## Sprint 9: Checkpointing & Partial Re-execution
**Goal**: Enable "rerun from here".

**Backend**
- Persist node outputs (small blobs OK).
- Resume logic:
  - Skip completed nodes
- Event support.

**Frontend**
- "Re-run from node" action.

**Acceptance criteria**
- Partial re-run works.
- State consistent.

---

## Sprint 10: Scheduling (Cron Only)
**Goal**: Hands-off execution.

**Backend**
- Cron scheduler.
- Schedule -> Run creation.
- DB-backed scheduling.

**Frontend**
- Schedule CRUD UI.

**Acceptance criteria**
- Scheduled runs trigger.
- Visible in UI.

---

## Sprint 11: Hardening & DX Polish (Optional)
**Goal**: Make it usable by other engineers.

**Backend**
- Error messages.
- Metrics hooks.
- Cleanup APIs.

**Frontend**
- UX polish.
- Empty/error states.

**Acceptance criteria**
- Stable demo.
- Clear failure modes.

---

## Sprint 12: Design Debt & Future Hooks (Optional)
**Goal**: Prepare for WASM & workers without implementing.

**Deliverables**
- WASM Node interface sketch.
- Worker split plan.
- Plugin boundaries.

**Acceptance criteria**
- Documented extension points.
- No code required.

---

## Meta notes (important)
- Each sprint is independently shippable.
- No sprint requires guessing future behavior.
- Typed contracts first, behavior second.
- UI follows backend truth, never leads it.

---

## Optional next steps
If you want, next I can:
- Turn this into agent tickets (one per sprint).
- Produce a repo tree snapshot per sprint.
- Write a "Definition of Done" rubric agents must satisfy before moving on.
