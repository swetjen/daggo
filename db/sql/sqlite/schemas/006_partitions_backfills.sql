CREATE TABLE partition_definitions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL,
    target_kind TEXT NOT NULL DEFAULT 'job',
    target_key TEXT NOT NULL DEFAULT '',
    definition_kind TEXT NOT NULL DEFAULT 'static',
    definition_json TEXT NOT NULL DEFAULT '{}',
    enabled INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (job_id) REFERENCES jobs (id) ON DELETE CASCADE,
    UNIQUE (job_id, target_kind, target_key)
);

CREATE INDEX idx_partition_definitions_job_target ON partition_definitions (job_id, target_kind, target_key, id);

CREATE TABLE partition_keys (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    partition_definition_id INTEGER NOT NULL,
    partition_key TEXT NOT NULL,
    sort_index INTEGER NOT NULL DEFAULT 0,
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (partition_definition_id) REFERENCES partition_definitions (id) ON DELETE CASCADE,
    UNIQUE (partition_definition_id, partition_key)
);

CREATE INDEX idx_partition_keys_definition_sort ON partition_keys (partition_definition_id, sort_index, partition_key, id);
CREATE INDEX idx_partition_keys_definition_active ON partition_keys (partition_definition_id, is_active, sort_index, partition_key, id);

CREATE TABLE backfills (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    backfill_key TEXT NOT NULL UNIQUE,
    job_id INTEGER NOT NULL,
    partition_definition_id INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'requested',
    selection_mode TEXT NOT NULL DEFAULT 'subset',
    selection_json TEXT NOT NULL DEFAULT '{}',
    triggered_by TEXT NOT NULL DEFAULT '',
    policy_mode TEXT NOT NULL DEFAULT 'multi_run',
    max_partitions_per_run INTEGER NOT NULL DEFAULT 1,
    requested_partition_count INTEGER NOT NULL DEFAULT 0,
    requested_run_count INTEGER NOT NULL DEFAULT 0,
    completed_partition_count INTEGER NOT NULL DEFAULT 0,
    failed_partition_count INTEGER NOT NULL DEFAULT 0,
    error_message TEXT NOT NULL DEFAULT '',
    started_at TEXT NOT NULL DEFAULT '',
    completed_at TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (job_id) REFERENCES jobs (id) ON DELETE CASCADE
);

CREATE INDEX idx_backfills_job_created ON backfills (job_id, created_at, id);
CREATE INDEX idx_backfills_status_created ON backfills (status, created_at, id);

CREATE TABLE backfill_partitions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    backfill_id INTEGER NOT NULL,
    partition_key TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'targeted',
    run_id INTEGER NOT NULL DEFAULT 0,
    error_message TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (backfill_id) REFERENCES backfills (id) ON DELETE CASCADE,
    UNIQUE (backfill_id, partition_key)
);

CREATE INDEX idx_backfill_partitions_backfill_status ON backfill_partitions (backfill_id, status, id);
CREATE INDEX idx_backfill_partitions_run_id ON backfill_partitions (run_id, id);

CREATE TABLE run_partition_targets (
    run_id INTEGER PRIMARY KEY,
    partition_definition_id INTEGER NOT NULL DEFAULT 0,
    selection_mode TEXT NOT NULL DEFAULT 'single',
    partition_key TEXT NOT NULL DEFAULT '',
    range_start_key TEXT NOT NULL DEFAULT '',
    range_end_key TEXT NOT NULL DEFAULT '',
    partition_subset_json TEXT NOT NULL DEFAULT '[]',
    tags_json TEXT NOT NULL DEFAULT '{}',
    backfill_key TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (run_id) REFERENCES runs (id) ON DELETE CASCADE
);

CREATE INDEX idx_run_partition_targets_backfill ON run_partition_targets (backfill_key, run_id);

CREATE TABLE run_system_tags (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    tag_key TEXT NOT NULL,
    tag_value TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (run_id) REFERENCES runs (id) ON DELETE CASCADE,
    UNIQUE (run_id, tag_key)
);

CREATE INDEX idx_run_system_tags_lookup ON run_system_tags (tag_key, tag_value, run_id);
