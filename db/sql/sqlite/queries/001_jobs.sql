-- name: JobGetMany :many
SELECT id, job_key, display_name, description, default_params_json, created_at, updated_at
FROM jobs
ORDER BY id
LIMIT ? OFFSET ?;

-- name: JobCount :one
SELECT COUNT(1) AS total
FROM jobs;

-- name: JobGetByKey :one
SELECT id, job_key, display_name, description, default_params_json, created_at, updated_at
FROM jobs
WHERE job_key = ?;

-- name: JobGetByID :one
SELECT id, job_key, display_name, description, default_params_json, created_at, updated_at
FROM jobs
WHERE id = ?;

-- name: JobDeleteByID :exec
DELETE FROM jobs
WHERE id = ?;

-- name: JobUpsert :one
INSERT INTO jobs (job_key, display_name, description, default_params_json)
VALUES (?, ?, ?, ?)
ON CONFLICT(job_key)
DO UPDATE SET
    display_name = excluded.display_name,
    description = excluded.description,
    default_params_json = excluded.default_params_json,
    updated_at = CURRENT_TIMESTAMP
RETURNING id, job_key, display_name, description, default_params_json, created_at, updated_at;

-- name: JobNodeDeleteByJobID :exec
DELETE FROM job_nodes
WHERE job_id = ?;

-- name: JobNodeCreate :one
INSERT INTO job_nodes (job_id, step_key, display_name, description, kind, metadata_json, sort_index)
VALUES (?, ?, ?, ?, ?, ?, ?)
RETURNING id, job_id, step_key, display_name, description, kind, metadata_json, sort_index, created_at;

-- name: JobNodeGetManyByJobID :many
SELECT id, job_id, step_key, display_name, description, kind, metadata_json, sort_index, created_at
FROM job_nodes
WHERE job_id = ?
ORDER BY sort_index, step_key;

-- name: JobEdgeDeleteByJobID :exec
DELETE FROM job_edges
WHERE job_id = ?;

-- name: JobEdgeCreate :one
INSERT INTO job_edges (job_id, from_step_key, to_step_key)
VALUES (?, ?, ?)
RETURNING id, job_id, from_step_key, to_step_key, created_at;

-- name: JobEdgeGetManyByJobID :many
SELECT id, job_id, from_step_key, to_step_key, created_at
FROM job_edges
WHERE job_id = ?
ORDER BY id;
