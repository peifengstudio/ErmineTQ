# ErmineTQ

A single-binary task queue and job runner for local Python services.

**Status: v0.1 — under active development**

## Why

Managing local Python tasks with crontab or Celery is painful:
- crontab has no status tracking or retry
- Celery requires Redis/RabbitMQ, has a broken dashboard, and is hard to configure

ErmineTQ is a Go binary with SQLite storage and a built-in dashboard.
Python connects via HTTP in three lines.

## Quickstart

```bash
# Install (macOS)
brew install peifengstudio/tap/erminetq   # coming soon

# Or build from source
mise install
mise run build
./bin/erminetq server
```

```python
from erminetq import ErmineTQClient

client  = ErmineTQClient("http://localhost:8080")
task_id = client.submit("my_task", {"key": "value"})
result  = client.wait(task_id)
```

## Architecture

- **Go Engine** — HTTP API, task queue, worker pool, scheduler
- **SQLite WAL** — single-file storage, no external dependencies
- **Python Bridge** — Unix socket process pool for Python tasks
- **Dashboard** — embedded Tailwind + Alpine.js UI

See [docs/DESIGN.md](docs/DESIGN.md) for full design decisions.

## Development

```bash
mise install        # install Go 1.25
mise run dev        # start server
mise run test       # run tests
mise run build      # build binary to bin/erminetq
```

## Project Structure

```
cmd/server/           entry point
internal/store/       SQLite store layer
internal/queue/       priority queue + worker pool
internal/api/         HTTP JSON API
internal/bridge/      Python Bridge client
internal/scheduler/   cron + interval scheduler
internal/dashboard/   HTTP server + SSE + embedded UI
dashboard/dist/       frontend (embedded into binary)
docs/                 design documents
```

## Related

- [erminetq-python](https://github.com/peifengstudio/erminetq-python) — Python SDK
