-- name: RunCreate :one
INSERT INTO runs (run_key, job_id, status, triggered_by, params_json, queued_at, started_at, completed_at, parent_run_id, rerun_step_key, error_message)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
RETURNING id, run_key, job_id, status, triggered_by, params_json, queued_at, started_at, completed_at, parent_run_id, rerun_step_key, error_message, created_at, updated_at;

-- name: RunGetByID :one
SELECT id, run_key, job_id, status, triggered_by, params_json, queued_at, started_at, completed_at, parent_run_id, rerun_step_key, error_message, created_at, updated_at
FROM runs
WHERE id = $1;

-- name: RunGetManyByJobID :many
SELECT id, run_key, job_id, status, triggered_by, params_json, queued_at, started_at, completed_at, parent_run_id, rerun_step_key, error_message, created_at, updated_at
FROM runs
WHERE job_id = $1
ORDER BY id DESC
LIMIT $2 OFFSET $3;

-- name: RunCountByJobID :one
SELECT COUNT(1) AS total
FROM runs
WHERE job_id = $1;

-- name: RunGetMany :many
SELECT id, run_key, job_id, status, triggered_by, params_json, queued_at, started_at, completed_at, parent_run_id, rerun_step_key, error_message, created_at, updated_at
FROM runs
ORDER BY id DESC
LIMIT $1 OFFSET $2;

-- name: RunGetManyForRetentionPurge :many
SELECT id
FROM runs
WHERE completed_at IS NOT NULL
  AND completed_at < $1
  AND status IN ('success', 'failed', 'canceled', 'cancelled')
ORDER BY completed_at, id
LIMIT $2;

-- name: RunCount :one
SELECT COUNT(1) AS total
FROM runs;

-- name: RunGetManyJoinedJobs :many
SELECT r.id,
       r.run_key,
       r.job_id,
       j.job_key,
       r.status,
       r.triggered_by,
       r.params_json,
       r.queued_at,
       r.started_at,
       r.completed_at,
       r.parent_run_id,
       r.rerun_step_key,
       r.error_message,
       r.created_at,
       r.updated_at
FROM runs r
JOIN jobs j ON j.id = r.job_id
ORDER BY r.id DESC
LIMIT $1 OFFSET $2;

-- name: RunGetManyByJobIDJoinedJobs :many
SELECT r.id,
       r.run_key,
       r.job_id,
       j.job_key,
       r.status,
       r.triggered_by,
       r.params_json,
       r.queued_at,
       r.started_at,
       r.completed_at,
       r.parent_run_id,
       r.rerun_step_key,
       r.error_message,
       r.created_at,
       r.updated_at
FROM runs r
JOIN jobs j ON j.id = r.job_id
WHERE r.job_id = $1
ORDER BY r.id DESC
LIMIT $2 OFFSET $3;

-- name: RunGetByIDJoinedJobs :one
SELECT r.id,
       r.run_key,
       r.job_id,
       j.job_key,
       r.status,
       r.triggered_by,
       r.params_json,
       r.queued_at,
       r.started_at,
       r.completed_at,
       r.parent_run_id,
       r.rerun_step_key,
       r.error_message,
       r.created_at,
       r.updated_at
FROM runs r
JOIN jobs j ON j.id = r.job_id
WHERE r.id = $1;

-- name: RunUpdateForStart :one
UPDATE runs
SET status = $1,
    started_at = $2,
    updated_at = now()
WHERE id = $3
RETURNING id, run_key, job_id, status, triggered_by, params_json, queued_at, started_at, completed_at, parent_run_id, rerun_step_key, error_message, created_at, updated_at;

-- name: RunUpdateForComplete :one
UPDATE runs
SET status = $1,
    completed_at = $2,
    error_message = $3,
    updated_at = now()
WHERE id = $4
RETURNING id, run_key, job_id, status, triggered_by, params_json, queued_at, started_at, completed_at, parent_run_id, rerun_step_key, error_message, created_at, updated_at;

-- name: RunDeleteByID :exec
DELETE FROM runs
WHERE id = $1;

-- name: RunStepCreate :one
INSERT INTO run_steps (run_id, job_node_id, step_key, status, attempt, started_at, completed_at, duration_ms, output_json, error_message, log_excerpt)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
RETURNING id, run_id, job_node_id, step_key, status, attempt, started_at, completed_at, duration_ms, output_json, error_message, log_excerpt, created_at, updated_at;

-- name: RunStepGetManyByRunID :many
SELECT id, run_id, job_node_id, step_key, status, attempt, started_at, completed_at, duration_ms, output_json, error_message, log_excerpt, created_at, updated_at
FROM run_steps
WHERE run_id = $1
ORDER BY id;

-- name: RunStepGetByRunIDAndStepKey :one
SELECT id, run_id, job_node_id, step_key, status, attempt, started_at, completed_at, duration_ms, output_json, error_message, log_excerpt, created_at, updated_at
FROM run_steps
WHERE run_id = $1 AND step_key = $2;

-- name: RunStepUpdateForStart :one
UPDATE run_steps
SET status = $1,
    started_at = $2,
    updated_at = now()
WHERE run_id = $3 AND step_key = $4
RETURNING id, run_id, job_node_id, step_key, status, attempt, started_at, completed_at, duration_ms, output_json, error_message, log_excerpt, created_at, updated_at;

-- name: RunStepUpdateForComplete :one
UPDATE run_steps
SET status = $1,
    completed_at = $2,
    duration_ms = $3,
    output_json = $4,
    error_message = $5,
    log_excerpt = $6,
    updated_at = now()
WHERE run_id = $7 AND step_key = $8
RETURNING id, run_id, job_node_id, step_key, status, attempt, started_at, completed_at, duration_ms, output_json, error_message, log_excerpt, created_at, updated_at;

-- name: RunEventCreate :one
INSERT INTO run_events (run_id, step_key, event_type, level, message, event_data_json)
VALUES ($1, $2, $3, $4, $5, $6)
RETURNING id, run_id, step_key, event_type, level, message, event_data_json, created_at;

-- name: RunEventGetManyByRunID :many
SELECT id, run_id, step_key, event_type, level, message, event_data_json, created_at
FROM run_events
WHERE run_id = $1
ORDER BY id
LIMIT $2 OFFSET $3;

-- name: RunEventCountByRunID :one
SELECT COUNT(*)
FROM run_events
WHERE run_id = $1;
