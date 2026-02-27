# WILL_DEPLOY Drain Lock Plan

## Intent

Provide a simple deployment guard that prevents new work from starting and lets active work drain before process exit.

## Proposed Mechanism (MVP)

Use a sentinel file in runtime storage:

- Path: `runtime/WILL_DEPLOY`
- Presence means "drain mode enabled"

When present:

- Block new run creation (`RunCreate`, `RunRerunStepCreate`).
- Scheduler does not enqueue new scheduled runs.
- Executor/daemon does not claim new queued runs.
- Process exits only after active runs are drained (or grace timeout is reached).

## Suggested File Payload

Plain text or JSON, for operator clarity:

```json
{
  "reason": "deploy",
  "initiated_by": "ci",
  "initiated_at": "2026-02-25T22:00:00Z",
  "grace_seconds": 900
}
```

## Runtime Behavior

- Poll sentinel every 1-2s (or on each scheduler/claim tick).
- On first detection:
  - emit `deployment_drain_started` log/event
  - set local drain mode flag
- In drain mode:
  - no new claims
  - no scheduler run creation
  - RPC create endpoints return deterministic error (`deployment in progress; new runs blocked`)
- Exit criteria:
  - no active runs owned by this process
  - OR grace period exceeded, then force exit with explicit warning

## Why This Works

- Very simple to reason about.
- Good for single-host/single-daemon deployment flow.
- Minimal implementation complexity.

## Limits of Sentinel-File Approach

- Not cluster-safe by default (file is local to one host/container).
- Requires shared filesystem for multi-instance correctness.
- Can be fragile in ephemeral container filesystems.

## Recommended Long-Term Variant (Better)

Move drain lock to DB-backed control state:

- Example table: `deployment_control`
  - `is_draining`
  - `started_at`
  - `reason`
  - `requested_by`
- All web/daemon instances read same state.
- Works correctly for multi-instance topology without shared filesystem.

## Proposed Rollout

## Step 1 (fast)

- Implement local sentinel behavior in daemon/web.
- Use for controlled deploys on current single-host setup.

## Step 2 (hardening)

- Add DB-backed global drain flag.
- Keep sentinel as optional local override for emergency/manual ops.

## Step 3 (operator UX)

- Add admin endpoint:
  - `POST /rpc/admin/deploy-drain-start`
  - `POST /rpc/admin/deploy-drain-stop`
  - `GET /rpc/admin/deploy-drain-status`
- Surface drain status in UI topbar.

## Suggested Deployment Procedure

1. Enable drain mode (`runtime/WILL_DEPLOY` or DB flag).
2. Wait for active runs to finish (or timeout policy).
3. Deploy new web/daemon versions.
4. Disable drain mode.
5. Verify scheduler and claim loops resume.

## Recommendation

Use sentinel file now only as an MVP for local/single-host deploys.  
Prioritize DB-backed drain control before multi-instance or autoscaled deployments.

