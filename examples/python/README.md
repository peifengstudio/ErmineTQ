# Python SDK Examples

Demonstrates the ErmineTQ Python SDK (`sdk/python/`) with four example task handlers.

## Setup

```bash
cd examples/python
uv sync          # installs erminetq SDK + httpx
```

## Running

Three terminals:

```bash
# Terminal 1 — Go server
make dev

# Terminal 2 — Python pull worker (polls /api/worker/claim)
make example-py-worker

# Terminal 3 — submit tasks and wait for results
make example-py-submit
```

Or without Make:

```bash
# Terminal 2
cd examples/python
ERMINETQ_URL=http://localhost:8080 uv run python worker.py

# Terminal 3
cd examples/python
uv run python submit.py --addr http://localhost:8080
```

## Files

| File | Purpose |
|---|---|
| `worker.py` | Pull worker — registers handlers, polls ErmineTQ, executes tasks |
| `submit.py` | Submits example tasks and waits for results |
| `handlers/http_fetch.py` | `py_http_fetch` — fetch a URL via httpx |
| `handlers/file_io.py` | `py_file_write`, `py_file_read` — stdlib pathlib |
| `handlers/ollama.py` | `py_ollama_chat` — local Ollama inference |

## Adding a new handler

1. Create `handlers/my_handler.py` with `def handle_my_task(payload: dict) -> dict`
2. Register it in `handlers/__init__.py`: `HANDLERS["my_task_type"] = handle_my_task`
3. Restart `worker.py` — it will advertise the new type to ErmineTQ on next register
