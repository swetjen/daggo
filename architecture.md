# DAGGO Architecture

```text
┌─────────────────────────────────────────────┐
│                Frontend UI                  │
│         (React / Svelte, RPC client)         │
└─────────────────────────────────────────────┘
                     │
                     │ Typed RPC (Virtuous)
                     ▼
┌─────────────────────────────────────────────┐
│              API / Control Plane             │
│  - Job registry                              │
│  - Graph validation                          │
│  - Run orchestration                         │
│  - Event ingestion                           │
│  - Read models for UI                        │
└─────────────────────────────────────────────┘
                     │
                     │ sqlc
                     ▼
┌─────────────────────────────────────────────┐
│             Persistence Plane                │
│  - PostgreSQL                                │
│  - Event log                                 │
│  - Derived state                             │
└─────────────────────────────────────────────┘
                     │
                     │ in-process (v1)
                     ▼
┌─────────────────────────────────────────────┐
│              Execution Plane                 │
│  - Goroutine executor                        │
│  - Context propagation                       │
│  - Node runtime                              │
└─────────────────────────────────────────────┘
```

---

## Core Concepts

| Concept | Meaning |
| --- | --- |
| **Node** | Typed unit of computation |
| **Job** | Named, static DAG of Nodes |
| **Graph** | Validated dependency structure |
| **Run** | One execution of a Job |
| **Event** | Immutable record of something that happened |
| **Executor** | Runs Nodes in dependency order |

---

## 3. Node Model (Type-Driven)

### Definition (conceptual)
- A Node is defined by:
  - A Go function
  - Required `context.Context`
  - Zero or more typed inputs
  - Zero or more typed outputs
  - Optional metadata

**Example (conceptual, not final API)**
```go
func Clean(ctx context.Context, raw RawEvents) (CleanEvents, error)
```

### Rules
- Inputs and outputs must be structs.
- Dependencies are inferred by type matching.
- `context.Context` is mandatory.
- Node code is unrestricted:
  - Side effects allowed
  - Network access allowed
  - Panics allowed (captured as failure)

---

## 4. Graph Construction & Validation

### Registration phase (startup)
- Nodes are registered.
- Jobs are registered.
- Graph is constructed.
- Validation occurs.
- Failure -> process exits (mirrors `http.ServeMux`).

### Dependency resolution order
1. Explicit wiring (Job builder).
2. Type inference.

### Validation errors
- Multiple upstream outputs satisfy one input.
- No upstream output satisfies an input.

### Guarantees
- Graph is acyclic.
- All dependencies resolved.
- All Nodes callable with resolved inputs.
- All types known at compile time.
- Graph is immutable after registration.

---

## 5. Job Model

A Job is:
- A named graph
- Static
- Reusable
- Independent of schedules, sensors, partitions

**Mental model**
- Job = what can run
- Run = what did run

---

## 6. Execution Architecture

### Executor (v1)
- In-process
- Goroutine-based
- Deterministic ordering derived from DAG
- One executor per Run

### Execution flow
1. Create Run record.
2. Emit `RunStarted`.
3. For each Node (topologically sorted):
   - Wait for dependencies
   - Emit `NodeStarted`
   - Execute Node
   - Emit success or failure
4. Emit `RunCompleted`.

### Context handling
- Root context per Run.
- Derived contexts per Node.
- Cancellation propagates downward.

---

## 7. Event-Driven Persistence Model

### Event log (source of truth)
- All state transitions are events.
- Examples:
  - `RunStarted`
  - `NodeStarted`
  - `NodeOutputProduced`
  - `NodeFailed`
  - `RunCompleted`

### Event properties
- Append-only
- Timestamped
- Typed

### Derived state
Current state is derived from events:
- Run status
- Node status
- Latest outputs
- Execution timeline

Enables:
- Replay
- Partial re-execution
- Auditability

---

## 8. Database Architecture (Postgres)

### Core tables (conceptual)
- `jobs`
- `nodes`
- `job_nodes` (graph edges)
- `runs`
- `run_events`
- `node_outputs` (small blobs)

### Design bias
- Normalize definitions.
- Denormalize runtime state via queries.
- Keep writes simple, reads optimized.

### Access pattern
- Writes: event append.
- Reads: derived queries via `sqlc`.
- No ORM.
- No runtime schema mutation.

---

## 9. API / Control Plane

### RPC-only API (Virtuous)
- All frontend communication goes through typed RPC.

### Handler domains
- Jobs
- Runs
- Graphs
- Events
- Scheduling

### Responsibilities
- Build read models for UI.
- Run creation.
- Schedule creation.
- Avoid duplicated business logic.
- Core execution logic stays in domain packages.

---

## 10. UI Architecture

### Responsibilities
- Visualize graphs.
- Observe runs.
- Trigger runs.
- Inspect logs and outputs.

### Model
- Stateless.
- Polling (v1) or streaming (future).
- No client-side orchestration logic.

### Likely pages
- Job list
- Job detail (graph)
- Run list
- Run detail (timeline + logs)

---

## 11. Registration & Assembly Pattern

Inspired by `http.ServeMux`.

### Canonical assembly point
- Nodes
- Jobs
- Resources
- Schedules

### Benefits
- Predictable startup.
- Easy reasoning.
- Early failure.

### Avoids
- Magic `init()` scattering.
- Runtime graph mutation.

---

## 12. Extensibility Boundaries (Future-Safe)

### Planned (not implemented)
- WASM Nodes
- External executors
- Distributed workers
- Object storage plugins

### Architectural hooks
- Node interface abstraction
- Executor interface
- Output materialization abstraction
- Event schema stability

**Design rule**: future extensions must plug in without weakening type safety.

---

## 13. Architectural Invariants (Non-Negotiable)
- Typed inputs/outputs everywhere.
- No string-based wiring.
- No runtime DAG mutation.
- No hidden magic.
- Fail early.
- Event log is truth.

---

## 14. Why This Architecture Works
- Go-native: leans into `context`, goroutines, generics.
- Simple first: v1 is understandable in a week.
- Dagster-inspired, not Dagster-copied.
- UI-ready from day one.
- Scales conceptually without over-engineering.
