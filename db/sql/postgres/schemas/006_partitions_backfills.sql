CREATE TABLE partition_definitions (
    id BIGSERIAL PRIMARY KEY,
    job_id BIGINT NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    target_kind TEXT NOT NULL DEFAULT 'job',
    target_key TEXT NOT NULL DEFAULT '',
    definition_kind TEXT NOT NULL DEFAULT 'static',
    definition_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    enabled BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (job_id, target_kind, target_key)
);

CREATE INDEX idx_partition_definitions_job_target ON partition_definitions (job_id, target_kind, target_key, id);

CREATE TABLE partition_keys (
    id BIGSERIAL PRIMARY KEY,
    partition_definition_id BIGINT NOT NULL REFERENCES partition_definitions (id) ON DELETE CASCADE,
    partition_key TEXT NOT NULL,
    sort_index BIGINT NOT NULL DEFAULT 0,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (partition_definition_id, partition_key)
);

CREATE INDEX idx_partition_keys_definition_sort ON partition_keys (partition_definition_id, sort_index, partition_key, id);
CREATE INDEX idx_partition_keys_definition_active ON partition_keys (partition_definition_id, is_active, sort_index, partition_key, id);

CREATE TABLE backfills (
    id BIGSERIAL PRIMARY KEY,
    backfill_key TEXT NOT NULL UNIQUE,
    job_id BIGINT NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    partition_definition_id BIGINT NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'requested',
    selection_mode TEXT NOT NULL DEFAULT 'subset',
    selection_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    triggered_by TEXT NOT NULL DEFAULT '',
    policy_mode TEXT NOT NULL DEFAULT 'multi_run',
    max_partitions_per_run BIGINT NOT NULL DEFAULT 1,
    requested_partition_count BIGINT NOT NULL DEFAULT 0,
    requested_run_count BIGINT NOT NULL DEFAULT 0,
    completed_partition_count BIGINT NOT NULL DEFAULT 0,
    failed_partition_count BIGINT NOT NULL DEFAULT 0,
    error_message TEXT NOT NULL DEFAULT '',
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_backfills_job_created ON backfills (job_id, created_at, id);
CREATE INDEX idx_backfills_status_created ON backfills (status, created_at, id);

CREATE TABLE backfill_partitions (
    id BIGSERIAL PRIMARY KEY,
    backfill_id BIGINT NOT NULL REFERENCES backfills (id) ON DELETE CASCADE,
    partition_key TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'targeted',
    run_id BIGINT NOT NULL DEFAULT 0,
    error_message TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (backfill_id, partition_key)
);

CREATE INDEX idx_backfill_partitions_backfill_status ON backfill_partitions (backfill_id, status, id);
CREATE INDEX idx_backfill_partitions_run_id ON backfill_partitions (run_id, id);

CREATE TABLE run_partition_targets (
    run_id BIGINT PRIMARY KEY REFERENCES runs (id) ON DELETE CASCADE,
    partition_definition_id BIGINT NOT NULL DEFAULT 0,
    selection_mode TEXT NOT NULL DEFAULT 'single',
    partition_key TEXT NOT NULL DEFAULT '',
    range_start_key TEXT NOT NULL DEFAULT '',
    range_end_key TEXT NOT NULL DEFAULT '',
    partition_subset_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    tags_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    backfill_key TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_run_partition_targets_backfill ON run_partition_targets (backfill_key, run_id);

CREATE TABLE run_system_tags (
    id BIGSERIAL PRIMARY KEY,
    run_id BIGINT NOT NULL REFERENCES runs (id) ON DELETE CASCADE,
    tag_key TEXT NOT NULL,
    tag_value TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (run_id, tag_key)
);

CREATE INDEX idx_run_system_tags_lookup ON run_system_tags (tag_key, tag_value, run_id);
