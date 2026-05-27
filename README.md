# ErmineTQ

A single-binary task queue and job runner for local Python services.  
Go engine · SQLite WAL · embedded Dashboard · Python Bridge over Unix socket.

**Status: v0.1 — under active development**

---

## Why

Managing local Python tasks with crontab or Celery is painful:

| Tool | Problem |
|------|---------|
| **crontab** | No status tracking, no retry, silent failures |
| **Celery** | Requires Redis/RabbitMQ, broken Flower dashboard, heavy config |

ErmineTQ is a single Go binary. It stores everything in a local SQLite file
and serves a built-in dashboard. Python connects in three lines.

---

## Quickstart

```bash
# Build from source
git clone https://github.com/peifengstudio/erminetq
cd erminetq
make build

# Run the server (creates erminetq.db in the current directory)
./bin/erminetq server

# Or point it at a specific database
ERMINETQ_DB=./data/myapp.db ./bin/erminetq server
```

```python
from erminetq import ErmineTQClient

client  = ErmineTQClient("http://localhost:8080")
task_id = client.submit("my_task", {"key": "value"})
result  = client.wait(task_id)
```

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `ERMINETQ_DB` | `erminetq.db` | Path to the SQLite database file |

The `-db` flag on every subcommand overrides `ERMINETQ_DB` when both are set.

```bash
# All three are equivalent:
./bin/erminetq server
ERMINETQ_DB=erminetq.db ./bin/erminetq server
./bin/erminetq server -db erminetq.db

# Flag takes precedence over the env var:
ERMINETQ_DB=foo.db ./bin/erminetq server -db bar.db   # uses bar.db
```

---

## CLI Reference

```
erminetq <command> [flags]

Commands:
  server    Start the ErmineTQ server (auto-applies migrations on startup)
  migrate   Apply pending database migrations and exit

Flags (both commands):
  -db string   Path to SQLite database file
               Env: ERMINETQ_DB  (default: erminetq.db)
```

---

## Development

### Prerequisites

- Go 1.25+  (`mise install` if you use [mise](https://mise.jdx.dev))
- `golangci-lint` for linting (optional)

### Common tasks

```bash
make build        # compile → bin/erminetq
make run          # go run (no compile step), respects ERMINETQ_DB
make migrate      # apply migrations and exit
make test         # run all tests
make test-v       # verbose test output
make test-store   # run only internal/store tests
make test-cover   # generate + open HTML coverage report
make lint         # golangci-lint
make fmt          # gofmt in-place
make tidy         # go mod tidy
make clean        # remove bin/ and coverage.out
make clean-db     # remove local *.db files
make help         # list all targets
```

Use `ERMINETQ_DB` with any make target:

```bash
ERMINETQ_DB=./data/dev.db make run
ERMINETQ_DB=./data/dev.db make migrate
```

---

## Architecture

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

- **Go Engine** — HTTP API, task queue, worker pool, scheduler, heartbeat scanner
- **SQLite WAL** — single-file storage, zero external dependencies, single write goroutine
- **Python Bridge** — long-lived Python process pool reached over a Unix socket
- **Dashboard** — Tailwind + Alpine.js UI compiled into `dashboard/dist/` and embedded into the binary via `embed.FS`

See [docs/DESIGN.md](docs/DESIGN.md) for full design rationale.

---

## Project Structure

```
cmd/server/           main entry point — wires everything together
internal/store/       SQLite store layer — ALL state transitions go here
internal/queue/       in-memory priority queue + worker pool
internal/api/         HTTP JSON API handlers and types
internal/bridge/      Python Bridge Unix socket client
internal/scheduler/   cron + interval scheduler
internal/dashboard/   HTTP server + SSE broker + embedded static files
dashboard/dist/       frontend build output (embedded into binary)
docs/                 design documents
```

---

## Task Status Machine

```
queued → running → succeeded
                 → retrying → queued   (backoff via run_at)
                 → dead               (retries exhausted)
                 → halted  → queued   (resume)
                           → cancelled
                 → cancelled
restart: original → superseded, new task queued with parent_id
```

`failed` is **not** a task status — it exists only on individual `attempts`.

---

## Related

- [erminetq-python](https://github.com/peifengstudio/erminetq-python) — Python SDK
