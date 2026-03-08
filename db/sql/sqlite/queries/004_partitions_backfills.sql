-- name: PartitionDefinitionUpsert :one
INSERT INTO partition_definitions (
    job_id,
    target_kind,
    target_key,
    definition_kind,
    definition_json,
    enabled
)
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(job_id, target_kind, target_key)
DO UPDATE SET
    definition_kind = excluded.definition_kind,
    definition_json = excluded.definition_json,
    enabled = excluded.enabled,
    updated_at = CURRENT_TIMESTAMP
RETURNING id, job_id, target_kind, target_key, definition_kind, definition_json, enabled, created_at, updated_at;

-- name: PartitionDefinitionGetByID :one
SELECT id, job_id, target_kind, target_key, definition_kind, definition_json, enabled, created_at, updated_at
FROM partition_definitions
WHERE id = ?;

-- name: PartitionDefinitionGetByJobIDAndTarget :one
SELECT id, job_id, target_kind, target_key, definition_kind, definition_json, enabled, created_at, updated_at
FROM partition_definitions
WHERE job_id = ?
  AND target_kind = ?
  AND target_key = ?;

-- name: PartitionDefinitionGetManyByJobID :many
SELECT id, job_id, target_kind, target_key, definition_kind, definition_json, enabled, created_at, updated_at
FROM partition_definitions
WHERE job_id = ?
ORDER BY target_kind, target_key, id;

-- name: PartitionDefinitionDeleteByID :exec
DELETE FROM partition_definitions
WHERE id = ?;

-- name: PartitionKeyUpsert :one
INSERT INTO partition_keys (
    partition_definition_id,
    partition_key,
    sort_index,
    is_active
)
VALUES (?, ?, ?, ?)
ON CONFLICT(partition_definition_id, partition_key)
DO UPDATE SET
    sort_index = excluded.sort_index,
    is_active = excluded.is_active,
    updated_at = CURRENT_TIMESTAMP
RETURNING id, partition_definition_id, partition_key, sort_index, is_active, created_at, updated_at;

-- name: PartitionKeyGetByDefinitionIDAndKey :one
SELECT id, partition_definition_id, partition_key, sort_index, is_active, created_at, updated_at
FROM partition_keys
WHERE partition_definition_id = ?
  AND partition_key = ?;

-- name: PartitionKeyGetManyByDefinitionID :many
SELECT id, partition_definition_id, partition_key, sort_index, is_active, created_at, updated_at
FROM partition_keys
WHERE partition_definition_id = ?
ORDER BY sort_index, partition_key, id
LIMIT ? OFFSET ?;

-- name: PartitionKeyCountByDefinitionID :one
SELECT COUNT(1) AS total
FROM partition_keys
WHERE partition_definition_id = ?;

-- name: PartitionKeyGetManyByDefinitionIDAndKeys :many
SELECT id, partition_definition_id, partition_key, sort_index, is_active, created_at, updated_at
FROM partition_keys
WHERE partition_definition_id = ?
  AND partition_key IN (sqlc.slice('partition_keys'))
ORDER BY sort_index, partition_key, id;

-- name: PartitionKeyGetManyByDefinitionIDRange :many
SELECT keys.id,
       keys.partition_definition_id,
       keys.partition_key,
       keys.sort_index,
       keys.is_active,
       keys.created_at,
       keys.updated_at
FROM partition_keys keys
JOIN partition_keys start_key
  ON start_key.partition_definition_id = keys.partition_definition_id
 AND start_key.partition_key = ?
JOIN partition_keys end_key
  ON end_key.partition_definition_id = keys.partition_definition_id
 AND end_key.partition_key = ?
WHERE keys.partition_definition_id = ?
  AND keys.sort_index BETWEEN
    CASE
        WHEN start_key.sort_index <= end_key.sort_index THEN start_key.sort_index
        ELSE end_key.sort_index
    END
    AND
    CASE
        WHEN start_key.sort_index <= end_key.sort_index THEN end_key.sort_index
        ELSE start_key.sort_index
    END
ORDER BY keys.sort_index, keys.partition_key, keys.id;

-- name: PartitionKeyDeleteByDefinitionID :exec
DELETE FROM partition_keys
WHERE partition_definition_id = ?;

-- name: PartitionKeyDeleteByDefinitionIDAndKey :exec
DELETE FROM partition_keys
WHERE partition_definition_id = ?
  AND partition_key = ?;

-- name: RunPartitionTargetUpsert :one
INSERT INTO run_partition_targets (
    run_id,
    partition_definition_id,
    selection_mode,
    partition_key,
    range_start_key,
    range_end_key,
    partition_subset_json,
    tags_json,
    backfill_key
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(run_id)
DO UPDATE SET
    partition_definition_id = excluded.partition_definition_id,
    selection_mode = excluded.selection_mode,
    partition_key = excluded.partition_key,
    range_start_key = excluded.range_start_key,
    range_end_key = excluded.range_end_key,
    partition_subset_json = excluded.partition_subset_json,
    tags_json = excluded.tags_json,
    backfill_key = excluded.backfill_key,
    updated_at = CURRENT_TIMESTAMP
RETURNING run_id, partition_definition_id, selection_mode, partition_key, range_start_key, range_end_key, partition_subset_json, tags_json, backfill_key, created_at, updated_at;

-- name: RunPartitionTargetGetByRunID :one
SELECT run_id, partition_definition_id, selection_mode, partition_key, range_start_key, range_end_key, partition_subset_json, tags_json, backfill_key, created_at, updated_at
FROM run_partition_targets
WHERE run_id = ?;

-- name: RunPartitionTargetGetManyByBackfillKey :many
SELECT run_id, partition_definition_id, selection_mode, partition_key, range_start_key, range_end_key, partition_subset_json, tags_json, backfill_key, created_at, updated_at
FROM run_partition_targets
WHERE backfill_key = ?
ORDER BY run_id DESC
LIMIT ? OFFSET ?;

-- name: RunPartitionTargetCountByBackfillKey :one
SELECT COUNT(1) AS total
FROM run_partition_targets
WHERE backfill_key = ?;

-- name: RunPartitionTargetDeleteByRunID :exec
DELETE FROM run_partition_targets
WHERE run_id = ?;

-- name: RunSystemTagUpsert :one
INSERT INTO run_system_tags (run_id, tag_key, tag_value)
VALUES (?, ?, ?)
ON CONFLICT(run_id, tag_key)
DO UPDATE SET
    tag_value = excluded.tag_value
RETURNING id, run_id, tag_key, tag_value, created_at;

-- name: RunSystemTagGetManyByRunID :many
SELECT id, run_id, tag_key, tag_value, created_at
FROM run_system_tags
WHERE run_id = ?
ORDER BY tag_key;

-- name: RunSystemTagGetManyByTagKeyAndValue :many
SELECT id, run_id, tag_key, tag_value, created_at
FROM run_system_tags
WHERE tag_key = ?
  AND tag_value = ?
ORDER BY run_id DESC
LIMIT ? OFFSET ?;

-- name: RunSystemTagCountByTagKeyAndValue :one
SELECT COUNT(1) AS total
FROM run_system_tags
WHERE tag_key = ?
  AND tag_value = ?;

-- name: RunSystemTagDeleteByRunID :exec
DELETE FROM run_system_tags
WHERE run_id = ?;

-- name: BackfillCreate :one
INSERT INTO backfills (
    backfill_key,
    job_id,
    partition_definition_id,
    status,
    selection_mode,
    selection_json,
    triggered_by,
    policy_mode,
    max_partitions_per_run,
    requested_partition_count,
    requested_run_count,
    completed_partition_count,
    failed_partition_count,
    error_message,
    started_at,
    completed_at
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
RETURNING id, backfill_key, job_id, partition_definition_id, status, selection_mode, selection_json, triggered_by, policy_mode, max_partitions_per_run, requested_partition_count, requested_run_count, completed_partition_count, failed_partition_count, error_message, started_at, completed_at, created_at, updated_at;

-- name: BackfillGetByID :one
SELECT id, backfill_key, job_id, partition_definition_id, status, selection_mode, selection_json, triggered_by, policy_mode, max_partitions_per_run, requested_partition_count, requested_run_count, completed_partition_count, failed_partition_count, error_message, started_at, completed_at, created_at, updated_at
FROM backfills
WHERE id = ?;

-- name: BackfillGetByKey :one
SELECT id, backfill_key, job_id, partition_definition_id, status, selection_mode, selection_json, triggered_by, policy_mode, max_partitions_per_run, requested_partition_count, requested_run_count, completed_partition_count, failed_partition_count, error_message, started_at, completed_at, created_at, updated_at
FROM backfills
WHERE backfill_key = ?;

-- name: BackfillGetManyByJobID :many
SELECT id, backfill_key, job_id, partition_definition_id, status, selection_mode, selection_json, triggered_by, policy_mode, max_partitions_per_run, requested_partition_count, requested_run_count, completed_partition_count, failed_partition_count, error_message, started_at, completed_at, created_at, updated_at
FROM backfills
WHERE job_id = ?
ORDER BY id DESC
LIMIT ? OFFSET ?;

-- name: BackfillCountByJobID :one
SELECT COUNT(1) AS total
FROM backfills
WHERE job_id = ?;

-- name: BackfillGetMany :many
SELECT id, backfill_key, job_id, partition_definition_id, status, selection_mode, selection_json, triggered_by, policy_mode, max_partitions_per_run, requested_partition_count, requested_run_count, completed_partition_count, failed_partition_count, error_message, started_at, completed_at, created_at, updated_at
FROM backfills
ORDER BY id DESC
LIMIT ? OFFSET ?;

-- name: BackfillCount :one
SELECT COUNT(1) AS total
FROM backfills;

-- name: BackfillUpdateStatus :one
UPDATE backfills
SET status = ?,
    requested_partition_count = ?,
    requested_run_count = ?,
    completed_partition_count = ?,
    failed_partition_count = ?,
    error_message = ?,
    started_at = ?,
    completed_at = ?,
    updated_at = CURRENT_TIMESTAMP
WHERE id = ?
RETURNING id, backfill_key, job_id, partition_definition_id, status, selection_mode, selection_json, triggered_by, policy_mode, max_partitions_per_run, requested_partition_count, requested_run_count, completed_partition_count, failed_partition_count, error_message, started_at, completed_at, created_at, updated_at;

-- name: BackfillDeleteByID :exec
DELETE FROM backfills
WHERE id = ?;

-- name: BackfillPartitionUpsert :one
INSERT INTO backfill_partitions (
    backfill_id,
    partition_key,
    status,
    run_id,
    error_message
)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT(backfill_id, partition_key)
DO UPDATE SET
    status = excluded.status,
    run_id = excluded.run_id,
    error_message = excluded.error_message,
    updated_at = CURRENT_TIMESTAMP
RETURNING id, backfill_id, partition_key, status, run_id, error_message, created_at, updated_at;

-- name: BackfillPartitionUpdateStatusByBackfillIDAndPartitionKey :one
UPDATE backfill_partitions
SET status = ?,
    run_id = ?,
    error_message = ?,
    updated_at = CURRENT_TIMESTAMP
WHERE backfill_id = ?
  AND partition_key = ?
RETURNING id, backfill_id, partition_key, status, run_id, error_message, created_at, updated_at;

-- name: BackfillPartitionGetManyByBackfillID :many
SELECT id, backfill_id, partition_key, status, run_id, error_message, created_at, updated_at
FROM backfill_partitions
WHERE backfill_id = ?
ORDER BY id
LIMIT ? OFFSET ?;

-- name: BackfillPartitionGetManyByBackfillIDAndStatus :many
SELECT id, backfill_id, partition_key, status, run_id, error_message, created_at, updated_at
FROM backfill_partitions
WHERE backfill_id = ?
  AND status = ?
ORDER BY id
LIMIT ? OFFSET ?;

-- name: BackfillPartitionGetManyByRunID :many
SELECT id, backfill_id, partition_key, status, run_id, error_message, created_at, updated_at
FROM backfill_partitions
WHERE run_id = ?
ORDER BY id;

-- name: BackfillPartitionCountByBackfillID :one
SELECT COUNT(1) AS total
FROM backfill_partitions
WHERE backfill_id = ?;

-- name: BackfillPartitionCountByBackfillIDAndStatus :one
SELECT COUNT(1) AS total
FROM backfill_partitions
WHERE backfill_id = ?
  AND status = ?;

-- name: BackfillPartitionDeleteByBackfillID :exec
DELETE FROM backfill_partitions
WHERE backfill_id = ?;
