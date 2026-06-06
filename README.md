# ErmineTQ

A single-binary task queue and job runner for local Python services.  
Go engine · SQLite WAL · embedded Dashboard · Python SDK (pull worker).

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
from erminetq import Client, Worker

# Submit tasks
with Client("http://localhost:8080") as client:
    task_id = client.submit("send_email", {"to": "alice@example.com"})

# Run a worker
worker = Worker("http://localhost:8080", concurrency=4)

@worker.register("send_email")
def send_email(payload: dict) -> dict:
    ...
    return {"sent": True}

worker.run()  # blocks; Ctrl+C for graceful shutdown
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

## Dashboard

The embedded dashboard is a React SPA (Vite + Tailwind CSS v4 + Font Awesome).
It is compiled into `internal/dashboard/dist/` and embedded into the binary
via `go:embed`, so no separate web server is needed.

Open **http://localhost:8080** after starting the server.

### Dashboard development workflow

Two terminals, one command each:

```bash
# Terminal 1 — Go backend (hot-reload or plain run)
make dev          # or: make run

# Terminal 2 — Vite dev server with HMR (proxies /api → :8080)
make ui-dev       # opens http://localhost:5173
```

The Vite dev server proxies all `/api/*` requests to the Go backend at `:8080`,
so you get instant hot-module-replacement on the frontend while the real API
responds normally.

### Dashboard build commands

```bash
make deps-ui      # install npm dependencies (pnpm, Node 25+)
make ui-build     # compile dashboard → internal/dashboard/dist/
make ui-preview   # build + preview production bundle at :5174
make ui-fmt       # format all src/**/*.{ts,tsx,css} with Prettier
make ui-fmt-check # check formatting (exit non-zero if unformatted — CI)
make build        # ui-build → go build (full binary, dashboard included)
make build-go     # go build only (skips ui-build, for rapid Go iteration)
```

> **Note:** `make build` always rebuilds the dashboard first. Use `make build-go`
> when you only changed Go code and the frontend is already up to date.

### Dashboard tech stack

| Layer | Tool |
|-------|------|
| Framework | React 18 + TypeScript |
| Build | Vite 6 |
| Styles | CSS custom properties (design tokens) + Tailwind CSS v4 |
| Icons | Font Awesome 6 (free-solid + free-regular) |
| Fonts | Inter + JetBrains Mono (self-hosted via `@fontsource`) |
| Format | Prettier 3 + `prettier-plugin-tailwindcss` |
| Runtime | Node 25 + pnpm 10 (managed by mise) |

### How it is embedded

```
dashboard/src/           React source
      ↓ pnpm run build
internal/dashboard/dist/ compiled assets (gitignored, except .gitkeep)
      ↓ //go:embed all:dist
internal/dashboard/      Go package — Handler() serves the SPA
      ↓ mux.Handle("/", dashboard.Handler())
cmd/server/main.go       wired after /api/* routes
```

- `/api/*` requests are handled by the API layer and never reach the dashboard handler.
- Any other path either serves a matching static asset or falls back to `index.html`
  so the React router handles client-side navigation.
- `index.html` is served with `Cache-Control: no-cache` so new binary deployments
  are picked up immediately.

---

## Development

### Prerequisites

- Go 1.25+, Python 3.11+, uv, Node 25+, pnpm 10 — run `mise install`
- `golangci-lint` for linting (optional)
- `air` for hot-reload dev: `go install github.com/air-verse/air@latest`

### Common tasks

```bash
make deps         # download all Go module dependencies
make deps-ui      # install dashboard npm dependencies
make build        # build dashboard + compile → bin/erminetq
make build-go     # compile Go only (skip dashboard build)
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

# Dashboard
make ui-dev       # Vite HMR dev server (proxy → :8080)
make ui-build     # production build → internal/dashboard/dist/
make ui-fmt       # Prettier format
make ui-fmt-check # Prettier check (CI)

# Python SDK examples — server must be running first (make dev)
make example-py-worker  # Terminal 2: start Python pull worker
make example-py-submit  # Terminal 3: submit example tasks and print results
make example-submit     # Go-only: submit Go handler tasks
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
│       └── Go Worker  (goroutine)                    │
│  Pull Worker API  (/api/worker/claim + report)      │
│  Retry Scheduler + Heartbeat Scanner                │
│  Cron Scheduler (schedules → tasks)                 │
│  SQLite WAL (single file, single writer goroutine)  │
└──────────────────┬──────────────────────────────────┘
                   │ SSE + embed.FS
┌──────────────────▼──────────────────────────────────┐
│  Dashboard (Tailwind + Alpine.js, fully embedded)   │
│  Overview · Tasks · Task Detail · Workers · Schedules│
└─────────────────────────────────────────────────────┘

Python SDK worker (separate process, any machine on the same network)
  poll → POST /api/worker/claim → execute handler → POST /api/worker/attempts/{id}/succeed|fail
```

- **Go Engine** — HTTP API, task queue, worker pool, scheduler, heartbeat scanner
- **SQLite WAL** — single-file storage, zero external dependencies, single write goroutine
- **Python SDK** — pull worker: polls `/api/worker/claim`, executes handlers, reports results; no Unix socket or persistent connection needed
- **Dashboard** — React + Tailwind CSS v4 + Font Awesome SPA compiled into `internal/dashboard/dist/` and embedded into the binary via `embed.FS`; served at `/` with SPA fallback

See [docs/DESIGN.md](docs/DESIGN.md) for full design rationale.

---

## Project Structure

```
cmd/server/           main entry point — wires everything together
internal/config/      TOML config loader and execution-limit types
internal/store/       SQLite store layer — ALL state transitions go here
internal/queue/       in-memory priority queue + worker pool
internal/api/         HTTP JSON API handlers and types
internal/scheduler/   cron + interval scheduler
internal/dashboard/   embedded SPA handler (embed.go) + SSE broker
internal/dashboard/dist/ frontend build output (gitignored; produced by make ui-build)
dashboard/            React/Vite frontend source (src/, vite.config.ts, package.json)
sdk/python/           Python SDK (erminetq package — Client + Worker)
examples/go/          Go example handlers and task submit script
examples/python/      Python SDK worker and task submit script
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

## Python SDK

The SDK lives in [`sdk/python/`](sdk/python/) and is installable as a path dependency:

```bash
# via uv (recommended)
uv add erminetq --path ./sdk/python

# via pip
pip install -e ./sdk/python
```
