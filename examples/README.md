# ErmineTQ Examples

End-to-end examples that test ErmineTQ with real task workloads.
All output files land in `examples/output/`.

---

## Task types covered

| Task type | Handler | What it does |
|-----------|---------|--------------|
| `go_http_fetch` | Go | GET a URL, save response metadata JSON |
| `go_file_write` | Go | Write text to `examples/output/` |
| `go_file_read` | Go | Read a file, log copy to `examples/output/` |
| `go_ollama_chat` | Go | Prompt local Ollama, save response text |
| `py_http_fetch` | Python | Same as above, via `httpx` |
| `py_file_write` | Python | Same as above, via `pathlib` |
| `py_file_read` | Python | Same as above, via `pathlib` |
| `py_ollama_chat` | Python | Same as above, via `httpx` |

---

## Quick start — Go workers only

```bash
# Terminal 1: start the example server (port 8081)
go run ./examples/go/server

# Terminal 2: submit tasks and watch results
go run ./examples/go/submit
```

## Quick start — Python Bridge

```bash
# Terminal 1: start the example server
go run ./examples/go/server

# Terminal 2: start the Python Bridge
cd examples/python
uv run python bridge/main.py

# Terminal 3: submit Python tasks
cd examples/python
uv run python submit.py
```

## Prerequisites

- Go 1.25+
- Python 3.11+ with [uv](https://docs.astral.sh/uv/) (`pip install uv`)
- **Ollama** (optional) — `ollama serve`, then pull a model:
  `ollama pull llama3.2:3b`
  If Ollama is not running the `*_ollama_chat` tasks will fail and retry.

## Output

All handlers write to `examples/output/`:

```
examples/output/
├── example.db                    Go server SQLite database
├── go_http_fetch_<ts>.json       HTTP fetch result
├── go_file_read_<ts>.txt         File read log
├── go_ollama_<ts>.txt            Ollama response
├── go_example_write.txt          File written by go_file_write
├── py_http_fetch_<task_id>.json  Python HTTP fetch result
├── py_file_read_<task_id>.txt    Python file read log
├── py_ollama_<task_id>.txt       Python Ollama response
├── py_example_write.txt          File written by py_file_write
└── py_summary.json               Python run summary
```

## Configuration

Both the Go server and Python Bridge respect environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `ERMINETQ_URL` | `http://localhost:8081` | Python Bridge → server URL |
| `BRIDGE_SOCKET` | `/tmp/erminetq_bridge.sock` | Unix socket path |
