# Python Examples

## Setup

```bash
cd examples/python
uv sync          # installs httpx
```

## bridge/main.py — Python Bridge server

Listens on a Unix socket, registers itself with ErmineTQ, and dispatches
incoming tasks to Python handlers.

```bash
# default: connects to http://localhost:8081, socket /tmp/erminetq_bridge.sock
uv run python bridge/main.py

# custom:
ERMINETQ_URL=http://localhost:8080 \
BRIDGE_SOCKET=/tmp/my_bridge.sock \
uv run python bridge/main.py
```

## submit.py — submit Python tasks

```bash
uv run python submit.py
uv run python submit.py --addr http://localhost:8081
```

## Handlers

| File | Task type | Library |
|---|---|---|
| `handlers/http_fetch.py` | `py_http_fetch` | `httpx` |
| `handlers/file_io.py` | `py_file_write`, `py_file_read` | stdlib `pathlib` |
| `handlers/ollama.py` | `py_ollama_chat` | `httpx` → Ollama API |

To add a new handler:
1. Create `handlers/my_handler.py` with `def handle_my_task(task_id, payload) -> dict`
2. Register it in `handlers/__init__.py`: `HANDLERS["my_task_type"] = handle_my_task`
