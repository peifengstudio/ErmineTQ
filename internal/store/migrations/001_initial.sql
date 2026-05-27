-- ErmineTQ v0.1 initial schema
-- Tables are ordered to satisfy foreign-key dependencies:
--   schedules → tasks → attempts / task_events
--   workers   → attempts

-- schedules must precede tasks (tasks.schedule_id → schedules.id)
CREATE TABLE schedules (
    id            TEXT    PRIMARY KEY,
    task_type     TEXT    NOT NULL,
    queue         TEXT    NOT NULL DEFAULT 'default',
    payload       JSON,
    cron_expr     TEXT,
    interval_secs INTEGER,
    enabled       BOOLEAN NOT NULL DEFAULT TRUE,
    last_run_at   DATETIME,
    next_run_at   DATETIME,
    created_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- workers must precede attempts (attempts.worker_id → workers.id)
CREATE TABLE workers (
    id                 TEXT    PRIMARY KEY,
    type               TEXT    NOT NULL,              -- 'go' | 'python'
    task_types         JSON    NOT NULL,              -- ["type_a", "type_b"]
    queue              TEXT    NOT NULL DEFAULT 'default',
    concurrency        INTEGER NOT NULL DEFAULT 1,
    current_task_count INTEGER NOT NULL DEFAULT 0,   -- maintained by Store only
    status             TEXT    NOT NULL DEFAULT 'idle',
    started_at         DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    heartbeat_at       DATETIME
);

CREATE TABLE tasks (
    id           TEXT    PRIMARY KEY,
    type         TEXT    NOT NULL,
    queue        TEXT    NOT NULL DEFAULT 'default',
    payload      JSON,
    status       TEXT    NOT NULL DEFAULT 'queued',
    priority     INTEGER NOT NULL DEFAULT 0,
    retry_count  INTEGER NOT NULL DEFAULT 0,
    max_retries  INTEGER NOT NULL DEFAULT 3,
    timeout_secs INTEGER,
    run_at       DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    parent_id    TEXT REFERENCES tasks(id),
    schedule_id  TEXT REFERENCES schedules(id),
    created_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_tasks_status_run_at ON tasks(status, run_at);
CREATE INDEX idx_tasks_priority      ON tasks(priority DESC, created_at ASC);

-- One row per execution attempt; a task accumulates rows here on retry/recovery.
-- heartbeat_at is updated directly and intentionally excluded from task_events.
CREATE TABLE attempts (
    id           TEXT    PRIMARY KEY,
    task_id      TEXT    NOT NULL REFERENCES tasks(id),
    attempt_num  INTEGER NOT NULL,       -- starts at 1
    worker_id    TEXT    REFERENCES workers(id),
    status       TEXT    NOT NULL,       -- running | succeeded | failed | cancelled
    result       JSON,
    error        TEXT,
    started_at   DATETIME,
    finished_at  DATETIME,
    heartbeat_at DATETIME
);

CREATE INDEX idx_attempts_task_id ON attempts(task_id);
CREATE INDEX idx_attempts_worker  ON attempts(worker_id, status);

-- State-transition events for the dashboard timeline and SSE.
-- Heartbeats are NOT written here; heartbeat_timeout events are.
CREATE TABLE task_events (
    id         TEXT PRIMARY KEY,
    task_id    TEXT NOT NULL REFERENCES tasks(id),
    event      TEXT NOT NULL,   -- queued|started|succeeded|retrying|dead|halted|cancelled|heartbeat_timeout
    detail     JSON,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_task_events_task_id ON task_events(task_id);
