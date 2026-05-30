package api

import "net/http"

// Handler wires the APIStore and SSE Broker to HTTP routes.
type Handler struct {
	store  APIStore
	broker *Broker
}

// NewHandler creates a Handler.
func NewHandler(store APIStore, broker *Broker) *Handler {
	return &Handler{store: store, broker: broker}
}

// Register mounts all API routes onto mux.
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/tasks", h.ListTasks)
	mux.HandleFunc("POST /api/tasks", h.CreateTask)
	mux.HandleFunc("GET /api/tasks/{id}", h.GetTask)
	mux.HandleFunc("POST /api/tasks/{id}/control", h.ControlTask)
	mux.HandleFunc("GET /api/workers", h.ListWorkers)
	mux.HandleFunc("POST /api/workers/register", h.RegisterWorker)

	mux.HandleFunc("GET /api/schedules", h.ListSchedules)
	mux.HandleFunc("POST /api/schedules", h.CreateSchedule)
	mux.HandleFunc("PATCH /api/schedules/{id}", h.UpdateSchedule)
	mux.HandleFunc("POST /api/schedules/{id}/trigger", h.TriggerSchedule)

	mux.HandleFunc("GET /api/events", h.HandleEvents)
}
