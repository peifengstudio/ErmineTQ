// Package handlers contains example WorkerFunc implementations for ErmineTQ.
// Each function can be registered with queue.Registry.Register.
//
// Output files are written to examples/output/ relative to the working directory.
package handlers

import "github.com/peifengstudio/erminetq/internal/queue"

// Registrar is a subset of queue.Registry used by RegisterDefaults.
type Registrar interface {
	Register(taskType string, fn queue.WorkerFunc)
}

// RegisterDefaults registers all built-in example handlers.
// Call registry.Register with the same task type afterwards to override any entry.
func RegisterDefaults(r Registrar) {
	r.Register("go_http_fetch", HTTPFetch)
	r.Register("go_file_write", FileWrite)
	r.Register("go_file_read", FileRead)
	r.Register("go_ollama_chat", OllamaChat)
}
