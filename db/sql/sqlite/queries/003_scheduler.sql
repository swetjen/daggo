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

-- name: SchedulerScheduleStateGetByJobScheduleID :one
SELECT job_schedule_id, last_checked_at, last_enqueued_at, next_run_at, updated_at
FROM scheduler_schedule_state
WHERE job_schedule_id = ?;

-- name: SchedulerScheduleStateUpsert :one
INSERT INTO scheduler_schedule_state (
    job_schedule_id,
    last_checked_at,
    last_enqueued_at,
    next_run_at
)
VALUES (?, ?, ?, ?)
ON CONFLICT(job_schedule_id)
DO UPDATE SET
    last_checked_at = excluded.last_checked_at,
    last_enqueued_at = excluded.last_enqueued_at,
    next_run_at = excluded.next_run_at,
    updated_at = CURRENT_TIMESTAMP
RETURNING job_schedule_id, last_checked_at, last_enqueued_at, next_run_at, updated_at;

-- name: SchedulerScheduleRunsCreateIfAbsent :many
INSERT INTO scheduler_schedule_runs (
    job_schedule_id,
    scheduled_for,
    run_key,
    triggered_by
)
VALUES (?, ?, ?, ?)
ON CONFLICT(job_schedule_id, scheduled_for)
DO NOTHING
RETURNING id, job_schedule_id, scheduled_for, run_key, triggered_by, created_at, updated_at;

-- name: SchedulerScheduleRunUpdateByID :one
UPDATE scheduler_schedule_runs
SET run_key = ?,
    triggered_by = ?,
    updated_at = CURRENT_TIMESTAMP
WHERE id = ?
RETURNING id, job_schedule_id, scheduled_for, run_key, triggered_by, created_at, updated_at;

-- name: SchedulerScheduleRunDeleteByID :exec
DELETE FROM scheduler_schedule_runs
WHERE id = ?;

-- name: SchedulerScheduleGetEnabledMany :many
SELECT js.id,
       js.job_id,
       j.job_key,
       js.schedule_key,
       js.cron_expr,
       js.timezone,
       js.description
FROM job_schedules js
JOIN jobs j ON j.id = js.job_id
WHERE js.is_enabled = 1
ORDER BY j.job_key, js.schedule_key, js.id;
