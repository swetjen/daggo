-- name: SchedulerHeartbeatUpsert :one
INSERT INTO scheduler_heartbeats (
    scheduler_key,
    last_heartbeat_at,
    last_tick_started_at,
    last_tick_completed_at,
    last_error
)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT(scheduler_key)
DO UPDATE SET
    last_heartbeat_at = excluded.last_heartbeat_at,
    last_tick_started_at = excluded.last_tick_started_at,
    last_tick_completed_at = excluded.last_tick_completed_at,
    last_error = excluded.last_error,
    updated_at = now()
RETURNING scheduler_key, last_heartbeat_at, last_tick_started_at, last_tick_completed_at, last_error, updated_at;

-- name: SchedulerScheduleStateGetByJobKeyScheduleKey :one
SELECT job_key, schedule_key, last_checked_at, last_enqueued_at, next_run_at, updated_at
FROM scheduler_schedule_state
WHERE job_key = $1
  AND schedule_key = $2;

-- name: SchedulerScheduleStateUpsert :one
INSERT INTO scheduler_schedule_state (
    job_key,
    schedule_key,
    last_checked_at,
    last_enqueued_at,
    next_run_at
)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT(job_key, schedule_key)
DO UPDATE SET
    last_checked_at = excluded.last_checked_at,
    last_enqueued_at = excluded.last_enqueued_at,
    next_run_at = excluded.next_run_at,
    updated_at = now()
RETURNING job_key, schedule_key, last_checked_at, last_enqueued_at, next_run_at, updated_at;

-- name: SchedulerScheduleStateGetMany :many
SELECT job_key, schedule_key, last_checked_at, last_enqueued_at, next_run_at, updated_at
FROM scheduler_schedule_state
ORDER BY job_key, schedule_key;

-- name: SchedulerScheduleStateDeleteByJobKeyScheduleKey :exec
DELETE FROM scheduler_schedule_state
WHERE job_key = $1
  AND schedule_key = $2;

-- name: SchedulerScheduleRunsCreateIfAbsent :many
INSERT INTO scheduler_schedule_runs (
    job_key,
    schedule_key,
    scheduled_for,
    run_key,
    triggered_by
)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT(job_key, schedule_key, scheduled_for)
DO NOTHING
RETURNING id, job_key, schedule_key, scheduled_for, run_key, triggered_by, created_at, updated_at;

-- name: SchedulerScheduleRunUpdateByID :one
UPDATE scheduler_schedule_runs
SET run_key = $1,
    triggered_by = $2,
    updated_at = now()
WHERE id = $3
RETURNING id, job_key, schedule_key, scheduled_for, run_key, triggered_by, created_at, updated_at;

-- name: SchedulerScheduleRunDeleteByID :exec
DELETE FROM scheduler_schedule_runs
WHERE id = $1;

-- name: SchedulerScheduleRunGetDistinctMany :many
SELECT DISTINCT job_key, schedule_key
FROM scheduler_schedule_runs
ORDER BY job_key, schedule_key;

-- name: SchedulerScheduleRunsDeleteByJobKeyScheduleKey :exec
DELETE FROM scheduler_schedule_runs
WHERE job_key = $1
  AND schedule_key = $2;
