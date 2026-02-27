# DAGGO Execution Daemon Split Plan

## Why

Today, DAGGO runs scheduler and executor inside the web/admin process.  
That makes deployments risky: restarting web can interrupt in-flight execution.

## Goals

- Web/UI deployments do not interrupt active runs.
- Execution ownership is durable and recoverable.
- Run behavior remains deterministic and observable.
- Job definitions remain code-defined, but execution is decoupled from web runtime.

## Non-Goals (initial)

- Full checkpoint/restart inside a single step function.
- Multi-region orchestration.
- Kubernetes-specific control plane.

## Target Topology

- `daggo-web`
  - serves embedded UI
  - serves read/write admin RPC
  - creates run rows
  - does not execute runs

- `daggo-daemon`
  - claims queued runs
  - executes steps
  - emits run/step events
  - runs scheduler loop

- Shared DB
  - source of truth for run state, events, claims, and schedule state

## Core Execution Invariants

- A run is executed by at most one active daemon claim at a time.
- Claim ownership expires without heartbeat.
- On daemon crash, stale claims are recoverable by other daemons.
- `run_events` remains append-only.
- Completed steps are not re-executed during recovery unless explicitly reset.

## Phased Delivery

## Phase 0: Durable Execution Semantics (monolith-compatible)

Introduce claim/lease semantics before splitting binaries.

- Add durable run claim state:
  - `claimed_by`
  - `claim_expires_at`
  - `claim_heartbeat_at`
  - `claim_attempt`
- Replace in-memory-only queue ownership with DB-driven claiming.
- Add heartbeat renewal loop while run executes.
- Add stale-claim sweeper/claim-takeover logic.

Outcome:
- Restart safety improves even before daemon split.

## Phase 1: Daemon Binary

- Add `cmd/daemon/main.go`.
- Move scheduler + executor startup from web process to daemon process.
- Keep existing RPC schema mostly unchanged for UI compatibility.
- Web still creates runs; daemon claims and executes.

Outcome:
- Deploying `daggo-web` does not stop execution.

## Phase 2: Recovery & Resume Policy

- On daemon startup:
  - find `runs.status in ('running','queued')` with stale/missing claim
  - claim and recover
- Recovery behavior:
  - `run_steps.status='success'` => reuse output
  - `run_steps.status='failed'/'skipped'` => preserve for historical run
  - `run_steps.status='running'` from dead daemon => reset to `pending` and emit recovery event

Outcome:
- In-flight runs survive daemon restarts at step boundary.

## Phase 3: Definition Version Pinning

To avoid behavior drift during rolling deploys:

- Persist `job_definition_hash` (or semantic version) on run creation.
- Daemon executes run only if local definition matches.
- If mismatched, leave queued or mark with explicit incompatibility state.

Outcome:
- Code deploys do not silently alter in-progress run semantics.

## Required Data Model Additions (high level)

- Extend `runs` or add `run_claims` table for leasing.
- Optional `run_recovery_events` can be represented using existing `run_events`.
- Optional `job_definition_hash` on `runs`.

## Operational Readiness Signals

- Daemon heartbeat table (similar to scheduler heartbeat).
- Claim age metrics.
- Recovery count metrics.
- Queue latency metrics (`queued_at` -> `started_at`).

## Rollout Strategy

1. Implement claims/heartbeats in current monolith runtime.
2. Validate recovery behavior in restart chaos tests.
3. Introduce daemon binary and run it in parallel with web execution disabled.
4. Switch scheduler ownership to daemon.
5. Remove in-process executor path from web runtime.

## Testing Strategy

- Claim exclusivity tests (two workers racing).
- Stale claim takeover tests.
- Restart recovery tests with partially completed runs.
- Scheduler dedupe/claim correctness under clock drift and retries.
- End-to-end deployment-drain tests.

