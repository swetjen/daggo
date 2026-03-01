CREATE TABLE jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_key TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL DEFAULT '',
    description TEXT NOT NULL DEFAULT '',
    default_params_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_jobs_created_at ON jobs (created_at, id);

CREATE TABLE job_nodes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL,
    step_key TEXT NOT NULL,
    display_name TEXT NOT NULL DEFAULT '',
    description TEXT NOT NULL DEFAULT '',
    kind TEXT NOT NULL DEFAULT 'op',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    sort_index INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (job_id) REFERENCES jobs (id) ON DELETE CASCADE,
    UNIQUE (job_id, step_key)
);

CREATE INDEX idx_job_nodes_job_sort ON job_nodes (job_id, sort_index, step_key);

CREATE TABLE job_edges (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL,
    from_step_key TEXT NOT NULL,
    to_step_key TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (job_id) REFERENCES jobs (id) ON DELETE CASCADE,
    UNIQUE (job_id, from_step_key, to_step_key)
);

CREATE INDEX idx_job_edges_job_to ON job_edges (job_id, to_step_key, from_step_key);
