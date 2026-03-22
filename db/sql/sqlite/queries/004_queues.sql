-- name: QueueUpsert :one
INSERT INTO queues (queue_key, display_name, description, route_path, load_mode, load_poll_every_seconds)
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT (queue_key) DO UPDATE SET
    display_name = excluded.display_name,
    description = excluded.description,
    route_path = excluded.route_path,
    load_mode = excluded.load_mode,
    load_poll_every_seconds = excluded.load_poll_every_seconds,
    updated_at = CURRENT_TIMESTAMP
RETURNING id, queue_key, display_name, description, route_path, load_mode, load_poll_every_seconds, created_at, updated_at;

-- name: QueueGetByKey :one
SELECT id, queue_key, display_name, description, route_path, load_mode, load_poll_every_seconds, created_at, updated_at
FROM queues
WHERE queue_key = ?;

-- name: QueueGetMany :many
SELECT id, queue_key, display_name, description, route_path, load_mode, load_poll_every_seconds, created_at, updated_at
FROM queues
ORDER BY queue_key
LIMIT ? OFFSET ?;

-- name: QueueCount :one
SELECT COUNT(*) FROM queues;

-- name: QueueJobDeleteByQueueID :exec
DELETE FROM queue_jobs
WHERE queue_id = ?;

-- name: QueueJobCreate :one
INSERT INTO queue_jobs (queue_id, job_id, sort_index)
VALUES (?, ?, ?)
RETURNING id, queue_id, job_id, sort_index, created_at;

-- name: QueueItemCreate :one
INSERT INTO queue_items (queue_id, queue_item_key, partition_key, status, external_key, payload_json, error_message, queued_at, started_at, completed_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
RETURNING id, queue_id, queue_item_key, partition_key, status, external_key, payload_json, error_message, queued_at, started_at, completed_at, created_at, updated_at;

-- name: QueueItemGetByIDJoinedQueues :one
SELECT qi.id,
       qi.queue_id,
       qi.queue_item_key,
       qi.partition_key,
       qi.status,
       qi.external_key,
       qi.payload_json,
       qi.error_message,
       qi.queued_at,
       qi.started_at,
       qi.completed_at,
       qi.created_at,
       qi.updated_at,
       q.queue_key,
       q.display_name AS queue_display_name
FROM queue_items qi
JOIN queues q ON q.id = qi.queue_id
WHERE qi.id = ?;

-- name: QueueItemGetManyByQueueID :many
SELECT id, queue_id, queue_item_key, partition_key, status, external_key, payload_json, error_message, queued_at, started_at, completed_at, created_at, updated_at
FROM queue_items
WHERE queue_id = ?
ORDER BY created_at DESC, id DESC
LIMIT ? OFFSET ?;

-- name: QueueItemCountByQueueID :one
SELECT COUNT(*)
FROM queue_items
WHERE queue_id = ?;

-- name: QueueItemUpdateLifecycleByID :one
UPDATE queue_items
SET status = ?,
    started_at = ?,
    completed_at = ?,
    error_message = ?,
    updated_at = CURRENT_TIMESTAMP
WHERE id = ?
RETURNING id, queue_id, queue_item_key, partition_key, status, external_key, payload_json, error_message, queued_at, started_at, completed_at, created_at, updated_at;

-- name: QueueItemStatusCountGetManyByQueueID :many
SELECT status, COUNT(*) AS total
FROM queue_items
WHERE queue_id = ?
GROUP BY status
ORDER BY status;

-- name: QueuePartitionGetManyByQueueID :many
WITH ranked AS (
    SELECT id,
           queue_id,
           queue_item_key,
           partition_key,
           status,
           external_key,
           payload_json,
           error_message,
           queued_at,
           started_at,
           completed_at,
           created_at,
           updated_at,
           ROW_NUMBER() OVER (
               PARTITION BY partition_key
               ORDER BY created_at DESC, id DESC
           ) AS rn
    FROM queue_items
    WHERE queue_id = ?
)
SELECT id, queue_id, queue_item_key, partition_key, status, external_key, payload_json, error_message, queued_at, started_at, completed_at, created_at, updated_at
FROM ranked
WHERE rn = 1
ORDER BY created_at DESC, id DESC
LIMIT ? OFFSET ?;

-- name: QueuePartitionCountByQueueID :one
SELECT COUNT(*)
FROM (
    SELECT DISTINCT partition_key
    FROM queue_items
    WHERE queue_id = ?
);

-- name: QueueItemRunCreate :one
INSERT INTO queue_item_runs (queue_item_id, job_id, run_id)
VALUES (?, ?, ?)
RETURNING id, queue_item_id, job_id, run_id, created_at;

-- name: QueueItemRunGetByRunID :one
SELECT id, queue_item_id, job_id, run_id, created_at
FROM queue_item_runs
WHERE run_id = ?;

-- name: QueueItemRunGetManyByQueueItemIDJoinedRuns :many
SELECT qir.id,
       qir.queue_item_id,
       qir.job_id,
       qir.run_id,
       qir.created_at,
       r.run_key,
       r.status AS run_status,
       r.triggered_by,
       r.queued_at,
       r.started_at,
       r.completed_at,
       r.parent_run_id,
       r.rerun_step_key,
       r.error_message AS run_error_message,
       j.job_key,
       j.display_name AS job_display_name
FROM queue_item_runs qir
JOIN runs r ON r.id = qir.run_id
JOIN jobs j ON j.id = qir.job_id
WHERE qir.queue_item_id = ?
ORDER BY qir.created_at, qir.id;

-- name: QueueItemStepMetadataUpsert :one
INSERT INTO queue_item_step_metadata (queue_item_id, job_id, run_id, step_key, metadata_json)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT (queue_item_id, job_id, step_key) DO UPDATE SET
    run_id = excluded.run_id,
    metadata_json = excluded.metadata_json,
    updated_at = CURRENT_TIMESTAMP
RETURNING id, queue_item_id, job_id, run_id, step_key, metadata_json, created_at, updated_at;

-- name: QueueItemStepMetadataGetManyByQueueItemID :many
SELECT id, queue_item_id, job_id, run_id, step_key, metadata_json, created_at, updated_at
FROM queue_item_step_metadata
WHERE queue_item_id = ?
ORDER BY job_id, step_key, id;
