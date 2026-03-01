CREATE TABLE jobs (
    id BIGSERIAL PRIMARY KEY,
    job_key TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL DEFAULT '',
    description TEXT NOT NULL DEFAULT '',
    default_params_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_jobs_created_at ON jobs (created_at, id);

CREATE TABLE job_nodes (
    id BIGSERIAL PRIMARY KEY,
    job_id BIGINT NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    step_key TEXT NOT NULL,
    display_name TEXT NOT NULL DEFAULT '',
    description TEXT NOT NULL DEFAULT '',
    kind TEXT NOT NULL DEFAULT 'op',
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    sort_index BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (job_id, step_key)
);

CREATE INDEX idx_job_nodes_job_sort ON job_nodes (job_id, sort_index, step_key);

CREATE TABLE job_edges (
    id BIGSERIAL PRIMARY KEY,
    job_id BIGINT NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    from_step_key TEXT NOT NULL,
    to_step_key TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (job_id, from_step_key, to_step_key)
);

CREATE INDEX idx_job_edges_job_to ON job_edges (job_id, to_step_key, from_step_key);
