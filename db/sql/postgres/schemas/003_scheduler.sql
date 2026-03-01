CREATE TABLE scheduler_heartbeats (
    scheduler_key TEXT PRIMARY KEY,
    last_heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_tick_started_at TIMESTAMPTZ,
    last_tick_completed_at TIMESTAMPTZ,
    last_error TEXT NOT NULL DEFAULT '',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE scheduler_schedule_state (
    job_schedule_id BIGINT PRIMARY KEY REFERENCES job_schedules (id) ON DELETE CASCADE,
    last_checked_at TIMESTAMPTZ,
    last_enqueued_at TIMESTAMPTZ,
    next_run_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE scheduler_schedule_runs (
    id BIGSERIAL PRIMARY KEY,
    job_schedule_id BIGINT NOT NULL REFERENCES job_schedules (id) ON DELETE CASCADE,
    scheduled_for TIMESTAMPTZ NOT NULL,
    run_key TEXT NOT NULL DEFAULT '',
    triggered_by TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (job_schedule_id, scheduled_for)
);

CREATE INDEX idx_scheduler_schedule_runs_schedule ON scheduler_schedule_runs (job_schedule_id, scheduled_for, id);
