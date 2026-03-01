-- name: SchedulerHeartbeatUpsert :one
INSERT INTO scheduler_heartbeats (
    scheduler_key,
    last_heartbeat_at,
    last_tick_started_at,
    last_tick_completed_at,
    last_error
)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT(scheduler_key)
DO UPDATE SET
    last_heartbeat_at = excluded.last_heartbeat_at,
    last_tick_started_at = excluded.last_tick_started_at,
    last_tick_completed_at = excluded.last_tick_completed_at,
    last_error = excluded.last_error,
    updated_at = CURRENT_TIMESTAMP
RETURNING scheduler_key, last_heartbeat_at, last_tick_started_at, last_tick_completed_at, last_error, updated_at;

-- name: SchedulerScheduleStateGetByJobKeyScheduleKey :one
SELECT job_key, schedule_key, last_checked_at, last_enqueued_at, next_run_at, updated_at
FROM scheduler_schedule_state
WHERE job_key = ?
  AND schedule_key = ?;

-- name: SchedulerScheduleStateUpsert :one
INSERT INTO scheduler_schedule_state (
    job_key,
    schedule_key,
    last_checked_at,
    last_enqueued_at,
    next_run_at
)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT(job_key, schedule_key)
DO UPDATE SET
    last_checked_at = excluded.last_checked_at,
    last_enqueued_at = excluded.last_enqueued_at,
    next_run_at = excluded.next_run_at,
    updated_at = CURRENT_TIMESTAMP
RETURNING job_key, schedule_key, last_checked_at, last_enqueued_at, next_run_at, updated_at;

-- name: SchedulerScheduleStateGetMany :many
SELECT job_key, schedule_key, last_checked_at, last_enqueued_at, next_run_at, updated_at
FROM scheduler_schedule_state
ORDER BY job_key, schedule_key;

-- name: SchedulerScheduleStateDeleteByJobKeyScheduleKey :exec
DELETE FROM scheduler_schedule_state
WHERE job_key = ?
  AND schedule_key = ?;

-- name: SchedulerScheduleRunsCreateIfAbsent :many
INSERT INTO scheduler_schedule_runs (
    job_key,
    schedule_key,
    scheduled_for,
    run_key,
    triggered_by
)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT(job_key, schedule_key, scheduled_for)
DO NOTHING
RETURNING id, job_key, schedule_key, scheduled_for, run_key, triggered_by, created_at, updated_at;

-- name: SchedulerScheduleRunUpdateByID :one
UPDATE scheduler_schedule_runs
SET run_key = ?,
    triggered_by = ?,
    updated_at = CURRENT_TIMESTAMP
WHERE id = ?
RETURNING id, job_key, schedule_key, scheduled_for, run_key, triggered_by, created_at, updated_at;

-- name: SchedulerScheduleRunDeleteByID :exec
DELETE FROM scheduler_schedule_runs
WHERE id = ?;

-- name: SchedulerScheduleRunGetDistinctMany :many
SELECT DISTINCT job_key, schedule_key
FROM scheduler_schedule_runs
ORDER BY job_key, schedule_key;

-- name: SchedulerScheduleRunsDeleteByJobKeyScheduleKey :exec
DELETE FROM scheduler_schedule_runs
WHERE job_key = ?
  AND schedule_key = ?;
