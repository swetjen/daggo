DROP TABLE IF EXISTS scheduler_schedule_runs;
DROP TABLE IF EXISTS scheduler_schedule_state;

CREATE TABLE scheduler_schedule_state (
    job_key TEXT NOT NULL,
    schedule_key TEXT NOT NULL,
    last_checked_at TIMESTAMPTZ,
    last_enqueued_at TIMESTAMPTZ,
    next_run_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (job_key, schedule_key)
);

CREATE TABLE scheduler_schedule_runs (
    id BIGSERIAL PRIMARY KEY,
    job_key TEXT NOT NULL,
    schedule_key TEXT NOT NULL,
    scheduled_for TIMESTAMPTZ NOT NULL,
    run_key TEXT NOT NULL DEFAULT '',
    triggered_by TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (job_key, schedule_key, scheduled_for)
);

CREATE INDEX idx_scheduler_schedule_runs_schedule ON scheduler_schedule_runs (job_key, schedule_key, scheduled_for, id);
