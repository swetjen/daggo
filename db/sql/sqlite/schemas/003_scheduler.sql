CREATE TABLE scheduler_heartbeats (
    scheduler_key TEXT PRIMARY KEY,
    last_heartbeat_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_tick_started_at TEXT NOT NULL DEFAULT '',
    last_tick_completed_at TEXT NOT NULL DEFAULT '',
    last_error TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE scheduler_schedule_state (
    job_schedule_id INTEGER PRIMARY KEY,
    last_checked_at TEXT NOT NULL DEFAULT '',
    last_enqueued_at TEXT NOT NULL DEFAULT '',
    next_run_at TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (job_schedule_id) REFERENCES job_schedules (id) ON DELETE CASCADE
);

CREATE TABLE scheduler_schedule_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_schedule_id INTEGER NOT NULL,
    scheduled_for TEXT NOT NULL,
    run_key TEXT NOT NULL DEFAULT '',
    triggered_by TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (job_schedule_id) REFERENCES job_schedules (id) ON DELETE CASCADE,
    UNIQUE (job_schedule_id, scheduled_for)
);

CREATE INDEX idx_scheduler_schedule_runs_schedule ON scheduler_schedule_runs (job_schedule_id, scheduled_for, id);
