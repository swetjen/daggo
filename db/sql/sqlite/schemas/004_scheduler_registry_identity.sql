DROP TABLE IF EXISTS scheduler_schedule_runs;
DROP TABLE IF EXISTS scheduler_schedule_state;

CREATE TABLE scheduler_schedule_state (
    job_key TEXT NOT NULL,
    schedule_key TEXT NOT NULL,
    last_checked_at TEXT NOT NULL DEFAULT '',
    last_enqueued_at TEXT NOT NULL DEFAULT '',
    next_run_at TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (job_key, schedule_key)
);

CREATE TABLE scheduler_schedule_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_key TEXT NOT NULL,
    schedule_key TEXT NOT NULL,
    scheduled_for TEXT NOT NULL,
    run_key TEXT NOT NULL DEFAULT '',
    triggered_by TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (job_key, schedule_key, scheduled_for)
);

CREATE INDEX idx_scheduler_schedule_runs_schedule ON scheduler_schedule_runs (job_key, schedule_key, scheduled_for, id);
