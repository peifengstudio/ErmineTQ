# Go Examples

## server — full ErmineTQ instance with Go handlers

`examples/go/server/main.go` boots a complete ErmineTQ stack on **:8081**
with four example task handlers registered:

| Handler func | Task type | Description |
|---|---|---|
| `handlers.HTTPFetch` | `go_http_fetch` | GET a URL, save metadata JSON |
| `handlers.FileWrite` | `go_file_write` | Write text to `examples/output/` |
| `handlers.FileRead` | `go_file_read` | Read a file, save log copy |
| `handlers.OllamaChat` | `go_ollama_chat` | Call local Ollama API |

```bash
go run ./examples/go/server
```

## submit — submit + poll tasks

`examples/go/submit/main.go` submits one task of each type, polls until
all reach a terminal status, and prints a summary table.

```bash
go run ./examples/go/submit
go run ./examples/go/submit -addr http://localhost:8081
```
