CREATE TABLE queues (
    id INTEGER PRIMARY KEY,
    queue_key TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL DEFAULT '',
    description TEXT NOT NULL DEFAULT '',
    route_path TEXT NOT NULL DEFAULT '',
    load_mode TEXT NOT NULL DEFAULT 'poll',
    load_poll_every_seconds BIGINT NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_queues_created_at ON queues (created_at, id);

CREATE TABLE queue_jobs (
    id INTEGER PRIMARY KEY,
    queue_id BIGINT NOT NULL REFERENCES queues (id) ON DELETE CASCADE,
    job_id BIGINT NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    sort_index BIGINT NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (queue_id, job_id)
);

CREATE INDEX idx_queue_jobs_queue_sort ON queue_jobs (queue_id, sort_index, job_id, id);

CREATE TABLE queue_items (
    id INTEGER PRIMARY KEY,
    queue_id BIGINT NOT NULL REFERENCES queues (id) ON DELETE CASCADE,
    queue_item_key TEXT NOT NULL UNIQUE,
    partition_key TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'queued',
    external_key TEXT NOT NULL DEFAULT '',
    payload_json TEXT NOT NULL DEFAULT '{}',
    error_message TEXT NOT NULL DEFAULT '',
    queued_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at TEXT NOT NULL DEFAULT '',
    completed_at TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_queue_items_queue_created ON queue_items (queue_id, created_at DESC, id DESC);
CREATE INDEX idx_queue_items_queue_status_created ON queue_items (queue_id, status, created_at DESC, id DESC);
CREATE INDEX idx_queue_items_queue_partition_created ON queue_items (queue_id, partition_key, created_at DESC, id DESC);

CREATE TABLE queue_item_runs (
    id INTEGER PRIMARY KEY,
    queue_item_id BIGINT NOT NULL REFERENCES queue_items (id) ON DELETE CASCADE,
    job_id BIGINT NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    run_id BIGINT NOT NULL UNIQUE REFERENCES runs (id) ON DELETE CASCADE,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (queue_item_id, job_id)
);

CREATE INDEX idx_queue_item_runs_item_job ON queue_item_runs (queue_item_id, job_id, run_id, id);

CREATE TABLE queue_item_step_metadata (
    id INTEGER PRIMARY KEY,
    queue_item_id BIGINT NOT NULL REFERENCES queue_items (id) ON DELETE CASCADE,
    job_id BIGINT NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    run_id BIGINT NOT NULL REFERENCES runs (id) ON DELETE CASCADE,
    step_key TEXT NOT NULL DEFAULT '',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (queue_item_id, job_id, step_key)
);

CREATE INDEX idx_queue_item_step_metadata_item_job ON queue_item_step_metadata (queue_item_id, job_id, step_key, id);
