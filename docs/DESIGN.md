# ErmineTQ — Design Document

> Core design decisions and technical rationale for ErmineTQ v0.1.

---

## Table of Contents

1. [Motivation](#1-motivation)
2. [Positioning](#2-positioning)
3. [Architecture Overview](#3-architecture-overview)
4. [Storage Design](#4-storage-design)
5. [Protocol: HTTP JSON + SSE](#5-protocol-http-json--sse)
6. [Language Boundary](#6-language-boundary)
7. [Worker Model](#7-worker-model)
8. [Task State Machine](#8-task-state-machine)
9. [Python Bridge](#9-python-bridge)
10. [Dashboard](#10-dashboard)
11. [Implementation Constraints](#11-implementation-constraints)
12. [Out of Scope](#12-out-of-scope)

---

## 1. Motivation

Managing many local Python services is a persistent operational pain point:

- **crontab** can trigger jobs on a schedule, but it does not track task state, surface failures, or provide retry control.
- **Celery** requires Redis or RabbitMQ, has a heavy configuration surface, often produces confusing worker state, and its Flower dashboard is not sufficient as an operation-first task control panel.
- Crawlers, data analysis jobs, local scripts, and Ollama inference tasks often run side by side, but there is usually no unified view of task state and no convenient way to pause, cancel, retry, or inspect them.

The goal of ErmineTQ is:

> **One binary, ready to run, Python integration in a few lines, and clear task visibility out of the box.**

---

## 2. Positioning

```text
ErmineTQ v0.1
Single-node Python Task Queue / Job Runner
Designed for crawlers, Ollama jobs, local scripts, and data analysis tasks

Core:
  Go server
  SQLite WAL
  HTTP JSON API
  SSE live updates
  Python SDK
  Python Bridge
  Embedded Dashboard

Storage:
  tasks
  attempts
  task_events
  workers
  schedules

Execution:
  Go Worker registry
  Python Bridge handlers
  cooperative cancellation
  heartbeat timeout recovery
```

ErmineTQ is not a general-purpose message queue. It is a local task control plane for Python-oriented workloads.

---

## 3. Architecture Overview

```
┌─────────────────────────────────────────────────────┐
│  Clients                                            │
│  Python SDK (httpx)         curl / browser          │
└────────────┬────────────────────────────────────────┘
             │ HTTP JSON API + SSE  :8080
┌────────────▼────────────────────────────────────────┐
│  Go Engine                                          │
│  HTTP Router → Task Queue → Worker Pool             │
│       ├── Go Worker  (goroutine)                    │
│       └── Python Bridge (Unix socket → py pool)     │
│  Retry Scheduler + Heartbeat Scanner                │
│  Cron Scheduler (schedules → tasks)                 │
│  SQLite WAL (single file, single writer goroutine)  │
└──────────────────┬──────────────────────────────────┘
                   │ SSE + embed.FS
┌──────────────────▼──────────────────────────────────┐
│  Dashboard (Tailwind + Alpine.js, fully embedded)   │
│  Overview · Tasks · Task Detail · Workers · Schedules│
└─────────────────────────────────────────────────────┘
```

The Go engine owns scheduling, state transitions, retries, heartbeat recovery, worker coordination, API serving, and dashboard delivery.

Python remains the primary execution environment for user code.

---

## 4. Storage Design

Zero external service dependency is a core constraint.

SQLite WAL, when used on a local SSD with a single-writer model, is sufficient for ErmineTQ v0.1's expected write workload. Actual throughput will be validated by benchmarks rather than promised in the design document.

All writes are serialized through a single writer goroutine. Reads may execute concurrently:

```go
type Store struct {
    db      *sql.DB
    writeCh chan writeOp
}
```

ErmineTQ uses `modernc.org/sqlite`, a pure-Go SQLite implementation. This avoids CGO and allows a single `go build` flow across macOS, Linux, and Windows.

---

### Schema

#### tasks — Main task table

```sql
CREATE TABLE tasks (
    id           TEXT PRIMARY KEY,
    type         TEXT NOT NULL,
    queue        TEXT NOT NULL DEFAULT 'default',
    payload      JSON,
    status       TEXT NOT NULL DEFAULT 'queued',
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
```

#### attempts — Per-execution attempt records

A task may be executed multiple times due to retries. The attempts table stores the full execution context of each attempt.

`failed` belongs only to an attempt status. It is not a final task status.

```sql
CREATE TABLE attempts (
    id           TEXT PRIMARY KEY,
    task_id      TEXT NOT NULL REFERENCES tasks(id),
    attempt_num  INTEGER NOT NULL,    -- starts from 1
    worker_id    TEXT REFERENCES workers(id),
    status       TEXT NOT NULL,       -- running / succeeded / failed / cancelled
    result       JSON,                -- small result only; large results should be stored externally
    error        TEXT,
    started_at   DATETIME,
    finished_at  DATETIME,
    heartbeat_at DATETIME             -- updated directly; heartbeat events are not written to task_events
);
CREATE INDEX idx_attempts_task_id ON attempts(task_id);
CREATE INDEX idx_attempts_worker  ON attempts(worker_id, status);
```

#### task_events — Task lifecycle event stream

This table records state transition points for the dashboard timeline and SSE updates.

Heartbeats are not written to this table to avoid event stream growth for long-running tasks.

```sql
CREATE TABLE task_events (
    id         TEXT PRIMARY KEY,
    task_id    TEXT NOT NULL REFERENCES tasks(id),
    event      TEXT NOT NULL,    -- queued / started / succeeded / retrying /
                                 -- dead / halted / cancelled / heartbeat_timeout
    detail     JSON,             -- extra information such as error, attempt_id, retry_count
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_task_events_task_id ON task_events(task_id);
```

#### workers — Registered workers

```sql
CREATE TABLE workers (
    id                 TEXT PRIMARY KEY,
    type               TEXT NOT NULL,        -- 'go' | 'python'
    task_types         JSON NOT NULL,
    queue              TEXT NOT NULL DEFAULT 'default',
    concurrency        INTEGER NOT NULL DEFAULT 1,
    current_task_count INTEGER NOT NULL DEFAULT 0,  -- derived field, maintained by the Store layer
    status             TEXT NOT NULL DEFAULT 'idle',
    started_at         DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    heartbeat_at       DATETIME
);
```

`current_task_count` is a derived field. It is maintained only by the Store state transition layer inside the same transaction as attempt and task updates. Business logic must not write this field directly.

Currently running tasks are queried from attempts:

```sql
SELECT * FROM attempts
WHERE worker_id = ? AND status = 'running';
```

#### schedules — Scheduled task definitions

```sql
CREATE TABLE schedules (
    id            TEXT PRIMARY KEY,
    task_type     TEXT NOT NULL,
    queue         TEXT NOT NULL DEFAULT 'default',
    payload       JSON,
    cron_expr     TEXT,
    interval_secs INTEGER,
    enabled       BOOLEAN NOT NULL DEFAULT TRUE,
    last_run_at   DATETIME,
    next_run_at   DATETIME,
    created_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
```

---

### Claim task: atomic task acquisition

When multiple goroutines are active, task claiming must be atomic.

ErmineTQ uses `UPDATE ... WHERE id = (SELECT ...)` to atomically claim one queued task. The single writer goroutine ensures serialized execution and prevents race conditions.

```sql
UPDATE tasks
SET    status     = 'running',
       updated_at = CURRENT_TIMESTAMP
WHERE  id = (
    SELECT id FROM tasks
    WHERE  status = 'queued'
      AND  queue  = ?
      AND  type   IN (/* task types supported by the worker */)
      AND  run_at <= CURRENT_TIMESTAMP
    ORDER BY priority DESC, created_at ASC
    LIMIT 1
)
RETURNING *;
```

If no row is returned, there is no available task and the worker waits before trying again.

---

### Retry scheduler

A background goroutine periodically moves due `retrying` tasks back to `queued`.

The claim SQL only claims tasks in the `queued` state.

```sql
UPDATE tasks
SET    status     = 'queued',
       updated_at = CURRENT_TIMESTAMP
WHERE  status = 'retrying'
  AND  run_at <= CURRENT_TIMESTAMP;
```

---

## 5. Protocol: HTTP JSON + SSE

```
GET    /api/tasks                    List tasks with status / type / queue / limit / offset filters
POST   /api/tasks                    Submit a task
GET    /api/tasks/:id                Get task detail, including attempts and events
POST   /api/tasks/:id/control        Control task: {action: halt|cancel|retry|restart}
GET    /api/workers                  List workers and worker state
POST   /api/workers/register         Register a worker, called by the Python Bridge on startup
GET    /api/schedules                List schedules
POST   /api/schedules                Create a schedule
PATCH  /api/schedules/:id            Update schedule configuration, such as enable/disable
POST   /api/schedules/:id/trigger    Trigger a schedule once immediately
GET    /api/events                   SSE stream for task_events
```

SSE event format:

```json
{
  "task_id":   "t_abc123",
  "event":     "succeeded",
  "detail":    {},
  "timestamp": "2026-05-26T09:00:00Z"
}
```

SSE is used only for real-time UI and SDK updates. It is not a reliable event delivery mechanism.

After reconnecting, clients should re-fetch current state through the REST API.

---

## 6. Language Boundary

Go's goroutine scheduler and memory model are well suited for the high-concurrency control plane.

Python is better suited for executing user business logic and accessing the Python ecosystem.

ErmineTQ uses the following language boundary:

```
Go Engine:
  scheduling
  task state machine
  retries
  heartbeat scanning
  worker coordination
  HTTP API
  dashboard serving
  SQLite persistence

Python:
  user task handlers
  crawlers
  data analysis
  local scripts
  Ollama / AI jobs
```

The Python SDK depends only on `httpx`:

```python
from erminetq import ErmineTQClient
client  = ErmineTQClient("http://localhost:8080")
task_id = client.submit("crawl_url", {"url": "https://example.com"})
result  = client.wait(task_id)
```

---

## 7. Worker Model

### Go Worker

A Go worker implements `WorkerFunc` and registers itself during startup:

```go
type WorkerFunc func(ctx context.Context, payload []byte) ([]byte, error)
registry.Register("crawl_url",  MyCrawlWorker)
registry.Register("run_script", MyScriptWorker)
```

### Python Worker

Python tasks are executed through the Python Bridge:

```python
@bridge.handler("analyze_data")
def analyze_data(payload: dict) -> dict:
    import pandas as pd
    return pd.DataFrame(payload["rows"]).describe().to_dict()
```

### Routing

```go
func (r *Registry) Dispatch(ctx context.Context, task *Task) ([]byte, error) {
    if r.pythonTypes[task.Type] {
        return r.bridge.Call(ctx, task)
    }
    return r.goWorkers[task.Type](ctx, task.Payload)
}
```

### Cooperative cancellation

Go workers receive cancellation through `context.WithCancel` and exit at the next `ctx.Done()` checkpoint.

Python Bridge tasks receive a cancellation signal over the socket and respond cooperatively at an appropriate point in the user code.

Both cancellation mechanisms are cooperative. Immediate termination is not guaranteed.

### Heartbeat timeout recovery

Running tasks update `attempts.heartbeat_at` every 5 seconds.

A background goroutine scans every 30 seconds. If a running attempt has not updated its heartbeat in time, ErmineTQ marks the attempt as failed, triggers retry logic, and writes one `task_events: heartbeat_timeout` event.

---

## 8. Task State Machine

### task.status

```
queued      Waiting for execution. Can be claimed when run_at <= now.
running     Claimed by a worker. A running attempt exists.
retrying    An attempt failed. The task is waiting for the next retry, with run_at updated by backoff.
succeeded   Completed successfully.
dead        Retry exhausted or non-retryable error. Manual intervention required.
halted      Paused manually. Can be resumed back to queued.
cancelled   Cancelled manually. Will not run again.
superseded  Replaced by a new task through restart.
```

### attempt.status

```
running
succeeded
failed
cancelled
```

`failed` exists only at the attempt level.

A task never enters a `failed` state:

- If an attempt fails and `retry_count < max_retries`, the task enters `retrying`.
- If retries are exhausted, the task enters `dead`.
- If the error is non-retryable, the task enters `dead`.

### State transitions

```
queued ──► running ──► succeeded
               │
               ├──► retrying ──► queued   (attempt failed, retry after backoff)
               ├──► dead                  (retry exhausted or non-retryable error)
               ├──► halted ──► queued     (resume)
               │         └──► cancelled
               └──► cancelled

restart:
  original task -> superseded
  new task      -> queued
  new.parent_id -> original task id
```

Each state transition updates `tasks`, `attempts`, `workers`, and `task_events` in a single transaction.

All transitions are performed through the Store layer. Invalid transitions are rejected by the Store layer.

---

## 9. Python Bridge

### Startup

The Python Bridge runs as a separate process. Its lifecycle is managed by the user:

```bash
uv run python -m myapp.bridge
```

### Registration

When the Bridge starts, it registers itself with the Go Engine:

```json
POST /api/workers/register
{
  "type":        "python",
  "task_types":  ["analyze_data", "train_model"],
  "queue":       "default",
  "concurrency": 4,
  "socket":      "/tmp/erminetq_bridge.sock"
}
```

### Execution path

```
Go Dispatch
    │ recognizes a Python task type
    │
Unix Socket /tmp/erminetq_bridge.sock
    │ JSON: {"task_id": "...", "type": "...", "payload": {...}}
    │
Python BridgeWorker
    long-lived process pool with preloaded dependencies
    │
handler execution → JSON result → returned to Go
```

### Restart behavior

If the Bridge process crashes, the Go Engine detects the broken socket connection.

Running attempts handled by that Bridge are marked as failed, and retry logic is triggered.

After restart, the Bridge registers itself again through `/api/workers/register`.

### Cancellation

Go sends a cancellation signal over the socket:

```json
{"type": "cancel", "task_id": "..."}
```

The Python process responds cooperatively and returns a cancellation result when possible.

---

## 10. Dashboard

All frontend assets are embedded through `embed.FS`.

Tailwind CSS and Alpine.js are bundled into `dist/` at build time. The released artifact remains a single binary with no runtime CDN dependency.

```go
//go:embed dashboard/dist/*
var static embed.FS
```

### Pages

- **Overview** — task counts by status, recent execution trend, worker health.
- **Tasks** — task list with status / type / queue filters, SSE live updates, and state-aware control buttons.
- **Task Detail** — payload, result, error, attempt history, task event timeline, and restart history chain.
- **Workers** — registered workers, concurrency, current task count, and heartbeat state.
- **Schedules** — scheduled tasks, next_run, enable/disable controls, and manual trigger.

---

## 11. Implementation Constraints

- **At-least-once execution**: worker crashes, heartbeat timeouts, and retries may cause duplicate execution. User task code should be idempotent when possible.
- **SSE is not reliable delivery**: SSE is only used for real-time notifications. It does not guarantee reliable event delivery. Clients must re-fetch state through the REST API after reconnecting.
- **Heartbeat does not write task events**: heartbeat only updates `attempts.heartbeat_at`. A `heartbeat_timeout` event is written only when a timeout is detected.
- **Large results are not stored directly**: `attempts.result` stores small JSON results or external references. Large outputs should be written by user code to the filesystem or object storage, with only references stored in ErmineTQ.
- **All state transitions go through the Store layer**: tasks, attempts, workers, and task_events are updated in a single transaction. Business logic must not mutate tables directly.
- **current_task_count is Store-maintained**: this derived field is updated only by Store-level state transitions and must not be written by business code.

---

## 12. Out of Scope

The following are explicitly out of scope for v0.1:

- gRPC transport
- Daily DB sharding
- Distributed deployment
- Hard kill cancellation
- Authentication / authorization
- Exact performance guarantees
