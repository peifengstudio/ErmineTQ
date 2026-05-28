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

# Run with defaults (creates erminetq.db in the current directory)
./bin/erminetq server

# Run with a config file
cp erminetq.example.toml erminetq.toml
./bin/erminetq server
```

```python
from erminetq import ErmineTQClient

client  = ErmineTQClient("http://localhost:8080")
task_id = client.submit("my_task", {"key": "value"})
result  = client.wait(task_id)
```

---

## Configuration

ErmineTQ is configured via a TOML file. Copy the annotated example to get started:

```bash
cp erminetq.example.toml erminetq.toml
```

By default ErmineTQ looks for `erminetq.toml` in the working directory.
**The file is optional** — if it is absent, compiled-in defaults are used.

### Config file structure

```toml
[db]
path = "erminetq.db"        # SQLite file path

[limits]
global = 128                # max total concurrent tasks

[queues.default]
limit = 32                  # max concurrent tasks in this queue

[queues.ollama]
limit = 2                   # GPU-bound queue, keep low

[task_types.ollama_generate]
queue = "ollama"
limit = 1                   # one inference job at a time
```

See [`erminetq.example.toml`](erminetq.example.toml) for the full reference with comments.

### Execution limits

ErmineTQ enforces three nested concurrency scopes. **All** applicable limits
must have capacity before a task is dispatched:

```
global limit
  └── queue limit        (if configured for that queue)
        └── task-type limit   (if configured for that type)
```

A limit of `0` at queue or task-type scope means **unlimited at that scope**
(global and any other applicable limit still apply).

### Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `ERMINETQ_CONFIG` | `erminetq.toml` | Path to the TOML config file |
| `ERMINETQ_DB` | `erminetq.db` | Path to the SQLite database file |

`ERMINETQ_DB` overrides `db.path` in the config file.

### Override precedence

Every setting follows the same priority chain (highest wins):

```
CLI flag  >  environment variable  >  config file  >  compiled-in default
```

Examples:

```bash
# Use a non-default config file
ERMINETQ_CONFIG=./config/prod.toml ./bin/erminetq server

# Override the database path at runtime without editing the config file
ERMINETQ_DB=./data/myapp.db ./bin/erminetq server

# CLI flag takes precedence over everything
ERMINETQ_DB=foo.db ./bin/erminetq server -db bar.db   # uses bar.db
```

---

## CLI Reference

```
erminetq <command> [flags]

Commands:
  server    Start the ErmineTQ server (also applies migrations on startup)
  migrate   Apply pending database migrations and exit
  version   Print version information and exit

Flags (server / migrate):
  -config string
        Path to TOML config file.
        Env: ERMINETQ_CONFIG  (default: erminetq.toml)

  -db string
        Override database path.
        Takes precedence over config file and ERMINETQ_DB.
```

---

## Development

### Prerequisites

- Go 1.25+  (`mise install` if you use [mise](https://mise.jdx.dev))
- `golangci-lint` for linting (optional)
- `air` for hot-reload dev: `go install github.com/air-verse/air@latest`

### Common tasks

```bash
make deps         # download all Go module dependencies
make build        # compile → bin/erminetq
make dev          # hot-reload server via air (requires air)
make run          # run server once via go run
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

Use env vars with any make target:

```bash
ERMINETQ_CONFIG=./config/dev.toml ERMINETQ_DB=./data/dev.db make run
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
internal/config/      TOML config loader and execution-limit types
internal/store/       SQLite store layer — ALL state transitions go here
internal/queue/       in-memory priority queue + worker pool
internal/api/         HTTP JSON API handlers and types
internal/bridge/      Python Bridge Unix socket client
internal/scheduler/   cron + interval scheduler
internal/dashboard/   HTTP server + SSE broker + embedded static files
dashboard/dist/       frontend build output (embedded into binary)
docs/                 design documents
erminetq.example.toml annotated reference configuration
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
