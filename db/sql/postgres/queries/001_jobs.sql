-- name: JobGetMany :many
SELECT id, job_key, display_name, description, default_params_json, created_at, updated_at
FROM jobs
ORDER BY id
LIMIT $1 OFFSET $2;

-- name: JobCount :one
SELECT COUNT(1) AS total
FROM jobs;

-- name: JobGetByKey :one
SELECT id, job_key, display_name, description, default_params_json, created_at, updated_at
FROM jobs
WHERE job_key = $1;

-- name: JobGetByID :one
SELECT id, job_key, display_name, description, default_params_json, created_at, updated_at
FROM jobs
WHERE id = $1;

-- name: JobDeleteByID :exec
DELETE FROM jobs
WHERE id = $1;

-- name: JobUpsert :one
INSERT INTO jobs (job_key, display_name, description, default_params_json)
VALUES ($1, $2, $3, $4)
ON CONFLICT(job_key)
DO UPDATE SET
    display_name = excluded.display_name,
    description = excluded.description,
    default_params_json = excluded.default_params_json,
    updated_at = now()
RETURNING id, job_key, display_name, description, default_params_json, created_at, updated_at;

-- name: JobNodeDeleteByJobID :exec
DELETE FROM job_nodes
WHERE job_id = $1;

-- name: JobNodeCreate :one
INSERT INTO job_nodes (job_id, step_key, display_name, description, kind, metadata_json, sort_index)
VALUES ($1, $2, $3, $4, $5, $6, $7)
RETURNING id, job_id, step_key, display_name, description, kind, metadata_json, sort_index, created_at;

-- name: JobNodeGetManyByJobID :many
SELECT id, job_id, step_key, display_name, description, kind, metadata_json, sort_index, created_at
FROM job_nodes
WHERE job_id = $1
ORDER BY sort_index, step_key;

-- name: JobEdgeDeleteByJobID :exec
DELETE FROM job_edges
WHERE job_id = $1;

-- name: JobEdgeCreate :one
INSERT INTO job_edges (job_id, from_step_key, to_step_key)
VALUES ($1, $2, $3)
RETURNING id, job_id, from_step_key, to_step_key, created_at;

-- name: JobEdgeGetManyByJobID :many
SELECT id, job_id, from_step_key, to_step_key, created_at
FROM job_edges
WHERE job_id = $1
ORDER BY id;
