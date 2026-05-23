-- name: RunCreate :one
INSERT INTO runs (run_key, job_id, status, triggered_by, params_json, queued_at, started_at, completed_at, parent_run_id, rerun_step_key, error_message)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
RETURNING id, run_key, job_id, status, triggered_by, params_json, queued_at, started_at, completed_at, parent_run_id, rerun_step_key, error_message, created_at, updated_at;

-- name: RunGetByID :one
SELECT id, run_key, job_id, status, triggered_by, params_json, queued_at, started_at, completed_at, parent_run_id, rerun_step_key, error_message, created_at, updated_at
FROM runs
WHERE id = ?;

-- name: RunGetManyByJobID :many
SELECT id, run_key, job_id, status, triggered_by, params_json, queued_at, started_at, completed_at, parent_run_id, rerun_step_key, error_message, created_at, updated_at
FROM runs
WHERE job_id = ?
ORDER BY id DESC
LIMIT ? OFFSET ?;

-- name: RunCountByJobID :one
SELECT COUNT(1) AS total
FROM runs
WHERE job_id = ?;

-- name: RunGetMany :many
SELECT id, run_key, job_id, status, triggered_by, params_json, queued_at, started_at, completed_at, parent_run_id, rerun_step_key, error_message, created_at, updated_at
FROM runs
ORDER BY id DESC
LIMIT ? OFFSET ?;

-- name: RunGetManyForRetentionPurge :many
SELECT id
FROM runs
WHERE completed_at != ''
  AND completed_at < ?
  AND status IN ('success', 'failed', 'canceled', 'cancelled')
ORDER BY completed_at, id
LIMIT ?;

-- name: RunCount :one
SELECT COUNT(1) AS total
FROM runs;

-- name: RunCountFiltered :one
SELECT COUNT(1) AS total
FROM runs r
JOIN jobs j ON j.id = r.job_id
WHERE (sqlc.arg('job_id') = 0 OR r.job_id = sqlc.arg('job_id'))
  AND (sqlc.arg('status') = '' OR r.status = sqlc.arg('status'))
  AND (
    sqlc.arg('search') = ''
    OR LOWER(r.run_key) LIKE '%' || sqlc.arg('search') || '%'
    OR LOWER(j.job_key) LIKE '%' || sqlc.arg('search') || '%'
    OR LOWER(r.triggered_by) LIKE '%' || sqlc.arg('search') || '%'
  )
  AND (
    sqlc.arg('quick_filter') = ''
    OR (
      (sqlc.arg('quick_filter') = 'backfills' AND LOWER(r.triggered_by) LIKE '%backfill%')
      OR (sqlc.arg('quick_filter') = 'queued' AND r.status IN ('queued', 'pending'))
      OR (sqlc.arg('quick_filter') = 'in_progress' AND r.status = 'running')
      OR (sqlc.arg('quick_filter') = 'failed' AND r.status = 'failed')
      OR (sqlc.arg('quick_filter') = 'scheduled' AND LOWER(r.triggered_by) LIKE '%schedule%')
    )
  )
  AND (
    sqlc.arg('window_hours') = 0
    OR julianday(r.queued_at) >= julianday('now', '-' || sqlc.arg('window_hours') || ' hours')
  );

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
LIMIT ? OFFSET ?;

-- name: RunGetManyFilteredJoinedJobs :many
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
WHERE (sqlc.arg('job_id') = 0 OR r.job_id = sqlc.arg('job_id'))
  AND (sqlc.arg('status') = '' OR r.status = sqlc.arg('status'))
  AND (
    sqlc.arg('search') = ''
    OR LOWER(r.run_key) LIKE '%' || sqlc.arg('search') || '%'
    OR LOWER(j.job_key) LIKE '%' || sqlc.arg('search') || '%'
    OR LOWER(r.triggered_by) LIKE '%' || sqlc.arg('search') || '%'
  )
  AND (
    sqlc.arg('quick_filter') = ''
    OR (
      (sqlc.arg('quick_filter') = 'backfills' AND LOWER(r.triggered_by) LIKE '%backfill%')
      OR (sqlc.arg('quick_filter') = 'queued' AND r.status IN ('queued', 'pending'))
      OR (sqlc.arg('quick_filter') = 'in_progress' AND r.status = 'running')
      OR (sqlc.arg('quick_filter') = 'failed' AND r.status = 'failed')
      OR (sqlc.arg('quick_filter') = 'scheduled' AND LOWER(r.triggered_by) LIKE '%schedule%')
    )
  )
  AND (
    sqlc.arg('window_hours') = 0
    OR julianday(r.queued_at) >= julianday('now', '-' || sqlc.arg('window_hours') || ' hours')
  )
ORDER BY CASE WHEN r.started_at != '' THEN r.started_at ELSE r.queued_at END DESC, r.id DESC
LIMIT sqlc.arg('limit') OFFSET sqlc.arg('offset');

-- name: RunGetManyFilteredJoinedJobsOldest :many
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
WHERE (sqlc.arg('job_id') = 0 OR r.job_id = sqlc.arg('job_id'))
  AND (sqlc.arg('status') = '' OR r.status = sqlc.arg('status'))
  AND (
    sqlc.arg('search') = ''
    OR LOWER(r.run_key) LIKE '%' || sqlc.arg('search') || '%'
    OR LOWER(j.job_key) LIKE '%' || sqlc.arg('search') || '%'
    OR LOWER(r.triggered_by) LIKE '%' || sqlc.arg('search') || '%'
  )
  AND (
    sqlc.arg('quick_filter') = ''
    OR (
      (sqlc.arg('quick_filter') = 'backfills' AND LOWER(r.triggered_by) LIKE '%backfill%')
      OR (sqlc.arg('quick_filter') = 'queued' AND r.status IN ('queued', 'pending'))
      OR (sqlc.arg('quick_filter') = 'in_progress' AND r.status = 'running')
      OR (sqlc.arg('quick_filter') = 'failed' AND r.status = 'failed')
      OR (sqlc.arg('quick_filter') = 'scheduled' AND LOWER(r.triggered_by) LIKE '%schedule%')
    )
  )
  AND (
    sqlc.arg('window_hours') = 0
    OR julianday(r.queued_at) >= julianday('now', '-' || sqlc.arg('window_hours') || ' hours')
  )
ORDER BY CASE WHEN r.started_at != '' THEN r.started_at ELSE r.queued_at END ASC, r.id ASC
LIMIT sqlc.arg('limit') OFFSET sqlc.arg('offset');

-- name: RunGetManyFilteredJoinedJobsDurationDesc :many
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
WHERE (sqlc.arg('job_id') = 0 OR r.job_id = sqlc.arg('job_id'))
  AND (sqlc.arg('status') = '' OR r.status = sqlc.arg('status'))
  AND (
    sqlc.arg('search') = ''
    OR LOWER(r.run_key) LIKE '%' || sqlc.arg('search') || '%'
    OR LOWER(j.job_key) LIKE '%' || sqlc.arg('search') || '%'
    OR LOWER(r.triggered_by) LIKE '%' || sqlc.arg('search') || '%'
  )
  AND (
    sqlc.arg('quick_filter') = ''
    OR (
      (sqlc.arg('quick_filter') = 'backfills' AND LOWER(r.triggered_by) LIKE '%backfill%')
      OR (sqlc.arg('quick_filter') = 'queued' AND r.status IN ('queued', 'pending'))
      OR (sqlc.arg('quick_filter') = 'in_progress' AND r.status = 'running')
      OR (sqlc.arg('quick_filter') = 'failed' AND r.status = 'failed')
      OR (sqlc.arg('quick_filter') = 'scheduled' AND LOWER(r.triggered_by) LIKE '%schedule%')
    )
  )
  AND (
    sqlc.arg('window_hours') = 0
    OR julianday(r.queued_at) >= julianday('now', '-' || sqlc.arg('window_hours') || ' hours')
  )
ORDER BY
  CASE
    WHEN r.started_at != '' AND r.completed_at != '' THEN CAST((julianday(r.completed_at) - julianday(r.started_at)) * 86400000 AS INTEGER)
    ELSE 0
  END DESC,
  CASE WHEN r.started_at != '' THEN r.started_at ELSE r.queued_at END DESC,
  r.id DESC
LIMIT sqlc.arg('limit') OFFSET sqlc.arg('offset');

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
WHERE r.job_id = ?
ORDER BY r.id DESC
LIMIT ? OFFSET ?;

-- name: RunGetManyByJobIDWithinWindowJoinedJobs :many
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
WHERE r.job_id = sqlc.arg('job_id')
  AND CASE WHEN r.started_at != '' THEN r.started_at ELSE r.queued_at END >= sqlc.arg('window_start')
  AND CASE WHEN r.started_at != '' THEN r.started_at ELSE r.queued_at END <= sqlc.arg('window_end')
ORDER BY CASE WHEN r.started_at != '' THEN r.started_at ELSE r.queued_at END DESC, r.id DESC
LIMIT sqlc.arg('limit') OFFSET sqlc.arg('offset');

-- name: RunGetManyFilteredJoinedJobsCursorNewest :many
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
WHERE (sqlc.arg('job_id') = 0 OR r.job_id = sqlc.arg('job_id'))
  AND (sqlc.arg('status') = '' OR r.status = sqlc.arg('status'))
  AND (
    sqlc.arg('search') = ''
    OR LOWER(r.run_key) LIKE '%' || sqlc.arg('search') || '%'
    OR LOWER(j.job_key) LIKE '%' || sqlc.arg('search') || '%'
    OR LOWER(r.triggered_by) LIKE '%' || sqlc.arg('search') || '%'
  )
  AND (
    sqlc.arg('quick_filter') = ''
    OR (
      (sqlc.arg('quick_filter') = 'backfills' AND LOWER(r.triggered_by) LIKE '%backfill%')
      OR (sqlc.arg('quick_filter') = 'queued' AND r.status IN ('queued', 'pending'))
      OR (sqlc.arg('quick_filter') = 'in_progress' AND r.status = 'running')
      OR (sqlc.arg('quick_filter') = 'failed' AND r.status = 'failed')
      OR (sqlc.arg('quick_filter') = 'scheduled' AND LOWER(r.triggered_by) LIKE '%schedule%')
    )
  )
  AND (
    sqlc.arg('window_hours') = 0
    OR julianday(r.queued_at) >= julianday('now', '-' || sqlc.arg('window_hours') || ' hours')
  )
  AND (
    sqlc.arg('cursor_at') = ''
    OR CASE WHEN r.started_at != '' THEN r.started_at ELSE r.queued_at END < sqlc.arg('cursor_at')
    OR (CASE WHEN r.started_at != '' THEN r.started_at ELSE r.queued_at END = sqlc.arg('cursor_at') AND r.id < sqlc.arg('cursor_id'))
  )
ORDER BY CASE WHEN r.started_at != '' THEN r.started_at ELSE r.queued_at END DESC, r.id DESC
LIMIT sqlc.arg('limit');

-- name: RunGetManyFilteredJoinedJobsCursorOldest :many
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
WHERE (sqlc.arg('job_id') = 0 OR r.job_id = sqlc.arg('job_id'))
  AND (sqlc.arg('status') = '' OR r.status = sqlc.arg('status'))
  AND (
    sqlc.arg('search') = ''
    OR LOWER(r.run_key) LIKE '%' || sqlc.arg('search') || '%'
    OR LOWER(j.job_key) LIKE '%' || sqlc.arg('search') || '%'
    OR LOWER(r.triggered_by) LIKE '%' || sqlc.arg('search') || '%'
  )
  AND (
    sqlc.arg('quick_filter') = ''
    OR (
      (sqlc.arg('quick_filter') = 'backfills' AND LOWER(r.triggered_by) LIKE '%backfill%')
      OR (sqlc.arg('quick_filter') = 'queued' AND r.status IN ('queued', 'pending'))
      OR (sqlc.arg('quick_filter') = 'in_progress' AND r.status = 'running')
      OR (sqlc.arg('quick_filter') = 'failed' AND r.status = 'failed')
      OR (sqlc.arg('quick_filter') = 'scheduled' AND LOWER(r.triggered_by) LIKE '%schedule%')
    )
  )
  AND (
    sqlc.arg('window_hours') = 0
    OR julianday(r.queued_at) >= julianday('now', '-' || sqlc.arg('window_hours') || ' hours')
  )
  AND (
    sqlc.arg('cursor_at') = ''
    OR CASE WHEN r.started_at != '' THEN r.started_at ELSE r.queued_at END > sqlc.arg('cursor_at')
    OR (CASE WHEN r.started_at != '' THEN r.started_at ELSE r.queued_at END = sqlc.arg('cursor_at') AND r.id > sqlc.arg('cursor_id'))
  )
ORDER BY CASE WHEN r.started_at != '' THEN r.started_at ELSE r.queued_at END ASC, r.id ASC
LIMIT sqlc.arg('limit');

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
WHERE r.id = ?;

-- name: RunUpdateForStart :one
UPDATE runs
SET status = ?,
    started_at = ?,
    updated_at = CURRENT_TIMESTAMP
WHERE id = ?
RETURNING id, run_key, job_id, status, triggered_by, params_json, queued_at, started_at, completed_at, parent_run_id, rerun_step_key, error_message, created_at, updated_at;

-- name: RunUpdateForComplete :one
UPDATE runs
SET status = ?,
    completed_at = ?,
    error_message = ?,
    updated_at = CURRENT_TIMESTAMP
WHERE id = ?
RETURNING id, run_key, job_id, status, triggered_by, params_json, queued_at, started_at, completed_at, parent_run_id, rerun_step_key, error_message, created_at, updated_at;

-- name: RunDeleteByID :exec
DELETE FROM runs
WHERE id = ?;

-- name: RunStepCreate :one
INSERT INTO run_steps (run_id, job_node_id, step_key, status, attempt, started_at, completed_at, duration_ms, output_json, error_message, log_excerpt)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
RETURNING id, run_id, job_node_id, step_key, status, attempt, started_at, completed_at, duration_ms, output_json, error_message, log_excerpt, created_at, updated_at;

-- name: RunStepGetManyByRunID :many
SELECT id, run_id, job_node_id, step_key, status, attempt, started_at, completed_at, duration_ms, output_json, error_message, log_excerpt, created_at, updated_at
FROM run_steps
WHERE run_id = ?
ORDER BY id;

-- name: RunStepGetByRunIDAndStepKey :one
SELECT id, run_id, job_node_id, step_key, status, attempt, started_at, completed_at, duration_ms, output_json, error_message, log_excerpt, created_at, updated_at
FROM run_steps
WHERE run_id = ? AND step_key = ?;

-- name: RunStepUpdateForStart :one
UPDATE run_steps
SET status = ?,
    started_at = ?,
    updated_at = CURRENT_TIMESTAMP
WHERE run_id = ? AND step_key = ?
RETURNING id, run_id, job_node_id, step_key, status, attempt, started_at, completed_at, duration_ms, output_json, error_message, log_excerpt, created_at, updated_at;

-- name: RunStepUpdateForComplete :one
UPDATE run_steps
SET status = ?,
    completed_at = ?,
    duration_ms = ?,
    output_json = ?,
    error_message = ?,
    log_excerpt = ?,
    updated_at = CURRENT_TIMESTAMP
WHERE run_id = ? AND step_key = ?
RETURNING id, run_id, job_node_id, step_key, status, attempt, started_at, completed_at, duration_ms, output_json, error_message, log_excerpt, created_at, updated_at;

-- name: RunEventCreate :one
INSERT INTO run_events (run_id, step_key, event_type, level, message, event_data_json)
VALUES (?, ?, ?, ?, ?, ?)
RETURNING id, run_id, step_key, event_type, level, message, event_data_json, created_at;

-- name: RunEventGetManyByRunID :many
SELECT id, run_id, step_key, event_type, level, message, event_data_json, created_at
FROM run_events
WHERE run_id = ?
ORDER BY id
LIMIT ? OFFSET ?;

-- name: RunEventCountByRunID :one
SELECT COUNT(*)
FROM run_events
WHERE run_id = ?;
