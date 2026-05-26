# ErmineTQ — Agent Instructions

## Project

ErmineTQ is a single-binary task queue and job runner for local Python services.
Go engine, SQLite WAL storage, HTTP JSON API, SSE, embedded Dashboard, Python Bridge.

Full design: docs/DESIGN.md — read this first before making any changes.

## Stack

- Go 1.25
- modernc.org/sqlite (pure Go, no CGO)
- HTTP JSON API on :8080
- SSE for live updates
- Tailwind CSS + Alpine.js (embedded into binary via embed.FS)
- Python Bridge via Unix socket

## Repository layout

```
cmd/server/         main entry point, wires everything together
internal/store/     SQLite layer — ALL state transitions go through here
internal/queue/     in-memory priority queue, worker pool, semaphore
internal/api/       HTTP handlers, request/response types
internal/bridge/    Unix socket client for Python Bridge
internal/scheduler/ cron + interval, writes tasks via store
internal/dashboard/ HTTP server, SSE broker, embed.FS static files
dashboard/dist/     frontend build output (embedded)
docs/               design documents
```

## Core constraints — read before writing any code

1. All state transitions (tasks, attempts, workers, task_events) must go through
   the Store layer in a single transaction. Never update these tables directly
   from handlers or workers.

2. Single write goroutine for SQLite. All writes are serialized through
   store.writeCh. Reads are concurrent (WAL mode).

3. task.status never becomes "failed". Only attempt.status can be "failed".
   Task failure flow: attempt failed → task retrying (if retries remain) → task dead.

4. At-least-once execution. Workers can crash. Tasks can run more than once.
   Business logic registered in the worker registry should be idempotent where possible.

5. SSE is not a reliable event stream. Clients must re-fetch state via REST API
   after reconnecting.

6. Cooperative cancellation only. halt/stop sends a signal; workers respond
   at their next checkpoint. No hard kill.

## Task status machine

```
queued → running → succeeded
                 → retrying → queued   (backoff via run_at)
                 → dead               (retries exhausted)
                 → halted → queued    (resume)
                          → cancelled
                 → cancelled
restart: original → superseded, new task queued with parent_id
```

## Attempt status

```
running → succeeded
        → failed
        → cancelled
```

## Key SQL patterns

Claim task (atomic, called by single write goroutine):
```sql
UPDATE tasks
SET status = 'running', updated_at = CURRENT_TIMESTAMP
WHERE id = (
    SELECT id FROM tasks
    WHERE status = 'queued'
      AND queue = ?
      AND type IN (...)
      AND run_at <= CURRENT_TIMESTAMP
    ORDER BY priority DESC, created_at ASC
    LIMIT 1
)
RETURNING *;
```

Retry scheduler (background goroutine, runs periodically):
```sql
UPDATE tasks
SET status = 'queued', updated_at = CURRENT_TIMESTAMP
WHERE status = 'retrying'
  AND run_at <= CURRENT_TIMESTAMP;
```

## Development workflow

Each internal package is developed independently. When implementing a package:
1. Read DESIGN.md for the relevant section
2. Write the interface/types first
3. Write tests alongside implementation
4. Wire into cmd/server/main.go last

## What is NOT in scope for v0.1

- gRPC
- daily DB sharding
- distributed deployment
- hard kill cancellation
- authentication
- exact performance guarantees
