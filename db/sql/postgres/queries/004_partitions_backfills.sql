-- name: PartitionDefinitionUpsert :one
INSERT INTO partition_definitions (
    job_id,
    target_kind,
    target_key,
    definition_kind,
    definition_json,
    enabled
)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT(job_id, target_kind, target_key)
DO UPDATE SET
    definition_kind = excluded.definition_kind,
    definition_json = excluded.definition_json,
    enabled = excluded.enabled,
    updated_at = now()
RETURNING id, job_id, target_kind, target_key, definition_kind, definition_json, enabled, created_at, updated_at;

-- name: PartitionDefinitionGetByID :one
SELECT id, job_id, target_kind, target_key, definition_kind, definition_json, enabled, created_at, updated_at
FROM partition_definitions
WHERE id = $1;

-- name: PartitionDefinitionGetByJobIDAndTarget :one
SELECT id, job_id, target_kind, target_key, definition_kind, definition_json, enabled, created_at, updated_at
FROM partition_definitions
WHERE job_id = $1
  AND target_kind = $2
  AND target_key = $3;

-- name: PartitionDefinitionGetManyByJobID :many
SELECT id, job_id, target_kind, target_key, definition_kind, definition_json, enabled, created_at, updated_at
FROM partition_definitions
WHERE job_id = $1
ORDER BY target_kind, target_key, id;

-- name: PartitionDefinitionDeleteByID :exec
DELETE FROM partition_definitions
WHERE id = $1;

-- name: PartitionKeyUpsert :one
INSERT INTO partition_keys (
    partition_definition_id,
    partition_key,
    sort_index,
    is_active
)
VALUES ($1, $2, $3, $4)
ON CONFLICT(partition_definition_id, partition_key)
DO UPDATE SET
    sort_index = excluded.sort_index,
    is_active = excluded.is_active,
    updated_at = now()
RETURNING id, partition_definition_id, partition_key, sort_index, is_active, created_at, updated_at;

-- name: PartitionKeyGetByDefinitionIDAndKey :one
SELECT id, partition_definition_id, partition_key, sort_index, is_active, created_at, updated_at
FROM partition_keys
WHERE partition_definition_id = $1
  AND partition_key = $2;

-- name: PartitionKeyGetManyByDefinitionID :many
SELECT id, partition_definition_id, partition_key, sort_index, is_active, created_at, updated_at
FROM partition_keys
WHERE partition_definition_id = $1
ORDER BY sort_index, partition_key, id
LIMIT $2 OFFSET $3;

-- name: PartitionKeyCountByDefinitionID :one
SELECT COUNT(1) AS total
FROM partition_keys
WHERE partition_definition_id = $1;

-- name: PartitionKeyGetManyByDefinitionIDAndKeys :many
SELECT id, partition_definition_id, partition_key, sort_index, is_active, created_at, updated_at
FROM partition_keys
WHERE partition_definition_id = $1
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
 AND start_key.partition_key = $1
JOIN partition_keys end_key
  ON end_key.partition_definition_id = keys.partition_definition_id
 AND end_key.partition_key = $2
WHERE keys.partition_definition_id = $3
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
WHERE partition_definition_id = $1;

-- name: PartitionKeyDeleteByDefinitionIDAndKey :exec
DELETE FROM partition_keys
WHERE partition_definition_id = $1
  AND partition_key = $2;

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
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
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
    updated_at = now()
RETURNING run_id, partition_definition_id, selection_mode, partition_key, range_start_key, range_end_key, partition_subset_json, tags_json, backfill_key, created_at, updated_at;

-- name: RunPartitionTargetGetByRunID :one
SELECT run_id, partition_definition_id, selection_mode, partition_key, range_start_key, range_end_key, partition_subset_json, tags_json, backfill_key, created_at, updated_at
FROM run_partition_targets
WHERE run_id = $1;

-- name: RunPartitionTargetGetManyByBackfillKey :many
SELECT run_id, partition_definition_id, selection_mode, partition_key, range_start_key, range_end_key, partition_subset_json, tags_json, backfill_key, created_at, updated_at
FROM run_partition_targets
WHERE backfill_key = $1
ORDER BY run_id DESC
LIMIT $2 OFFSET $3;

-- name: RunPartitionTargetCountByBackfillKey :one
SELECT COUNT(1) AS total
FROM run_partition_targets
WHERE backfill_key = $1;

-- name: RunPartitionTargetDeleteByRunID :exec
DELETE FROM run_partition_targets
WHERE run_id = $1;

-- name: RunSystemTagUpsert :one
INSERT INTO run_system_tags (run_id, tag_key, tag_value)
VALUES ($1, $2, $3)
ON CONFLICT(run_id, tag_key)
DO UPDATE SET
    tag_value = excluded.tag_value
RETURNING id, run_id, tag_key, tag_value, created_at;

-- name: RunSystemTagGetManyByRunID :many
SELECT id, run_id, tag_key, tag_value, created_at
FROM run_system_tags
WHERE run_id = $1
ORDER BY tag_key;

-- name: RunSystemTagGetManyByTagKeyAndValue :many
SELECT id, run_id, tag_key, tag_value, created_at
FROM run_system_tags
WHERE tag_key = $1
  AND tag_value = $2
ORDER BY run_id DESC
LIMIT $3 OFFSET $4;

-- name: RunSystemTagCountByTagKeyAndValue :one
SELECT COUNT(1) AS total
FROM run_system_tags
WHERE tag_key = $1
  AND tag_value = $2;

-- name: RunSystemTagDeleteByRunID :exec
DELETE FROM run_system_tags
WHERE run_id = $1;

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
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
RETURNING id, backfill_key, job_id, partition_definition_id, status, selection_mode, selection_json, triggered_by, policy_mode, max_partitions_per_run, requested_partition_count, requested_run_count, completed_partition_count, failed_partition_count, error_message, started_at, completed_at, created_at, updated_at;

-- name: BackfillGetByID :one
SELECT id, backfill_key, job_id, partition_definition_id, status, selection_mode, selection_json, triggered_by, policy_mode, max_partitions_per_run, requested_partition_count, requested_run_count, completed_partition_count, failed_partition_count, error_message, started_at, completed_at, created_at, updated_at
FROM backfills
WHERE id = $1;

-- name: BackfillGetByKey :one
SELECT id, backfill_key, job_id, partition_definition_id, status, selection_mode, selection_json, triggered_by, policy_mode, max_partitions_per_run, requested_partition_count, requested_run_count, completed_partition_count, failed_partition_count, error_message, started_at, completed_at, created_at, updated_at
FROM backfills
WHERE backfill_key = $1;

-- name: BackfillGetManyByJobID :many
SELECT id, backfill_key, job_id, partition_definition_id, status, selection_mode, selection_json, triggered_by, policy_mode, max_partitions_per_run, requested_partition_count, requested_run_count, completed_partition_count, failed_partition_count, error_message, started_at, completed_at, created_at, updated_at
FROM backfills
WHERE job_id = $1
ORDER BY id DESC
LIMIT $2 OFFSET $3;

-- name: BackfillCountByJobID :one
SELECT COUNT(1) AS total
FROM backfills
WHERE job_id = $1;

-- name: BackfillGetMany :many
SELECT id, backfill_key, job_id, partition_definition_id, status, selection_mode, selection_json, triggered_by, policy_mode, max_partitions_per_run, requested_partition_count, requested_run_count, completed_partition_count, failed_partition_count, error_message, started_at, completed_at, created_at, updated_at
FROM backfills
ORDER BY id DESC
LIMIT $1 OFFSET $2;

-- name: BackfillCount :one
SELECT COUNT(1) AS total
FROM backfills;

-- name: BackfillUpdateStatus :one
UPDATE backfills
SET status = $1,
    requested_partition_count = $2,
    requested_run_count = $3,
    completed_partition_count = $4,
    failed_partition_count = $5,
    error_message = $6,
    started_at = $7,
    completed_at = $8,
    updated_at = now()
WHERE id = $9
RETURNING id, backfill_key, job_id, partition_definition_id, status, selection_mode, selection_json, triggered_by, policy_mode, max_partitions_per_run, requested_partition_count, requested_run_count, completed_partition_count, failed_partition_count, error_message, started_at, completed_at, created_at, updated_at;

-- name: BackfillDeleteByID :exec
DELETE FROM backfills
WHERE id = $1;

-- name: BackfillPartitionUpsert :one
INSERT INTO backfill_partitions (
    backfill_id,
    partition_key,
    status,
    run_id,
    error_message
)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT(backfill_id, partition_key)
DO UPDATE SET
    status = excluded.status,
    run_id = excluded.run_id,
    error_message = excluded.error_message,
    updated_at = now()
RETURNING id, backfill_id, partition_key, status, run_id, error_message, created_at, updated_at;

-- name: BackfillPartitionUpdateStatusByBackfillIDAndPartitionKey :one
UPDATE backfill_partitions
SET status = $1,
    run_id = $2,
    error_message = $3,
    updated_at = now()
WHERE backfill_id = $4
  AND partition_key = $5
RETURNING id, backfill_id, partition_key, status, run_id, error_message, created_at, updated_at;

-- name: BackfillPartitionGetManyByBackfillID :many
SELECT id, backfill_id, partition_key, status, run_id, error_message, created_at, updated_at
FROM backfill_partitions
WHERE backfill_id = $1
ORDER BY id
LIMIT $2 OFFSET $3;

-- name: BackfillPartitionGetManyByBackfillIDAndStatus :many
SELECT id, backfill_id, partition_key, status, run_id, error_message, created_at, updated_at
FROM backfill_partitions
WHERE backfill_id = $1
  AND status = $2
ORDER BY id
LIMIT $3 OFFSET $4;

-- name: BackfillPartitionGetManyByRunID :many
SELECT id, backfill_id, partition_key, status, run_id, error_message, created_at, updated_at
FROM backfill_partitions
WHERE run_id = $1
ORDER BY id;

-- name: BackfillPartitionCountByBackfillID :one
SELECT COUNT(1) AS total
FROM backfill_partitions
WHERE backfill_id = $1;

-- name: BackfillPartitionCountByBackfillIDAndStatus :one
SELECT COUNT(1) AS total
FROM backfill_partitions
WHERE backfill_id = $1
  AND status = $2;

-- name: BackfillPartitionDeleteByBackfillID :exec
DELETE FROM backfill_partitions
WHERE backfill_id = $1;
