package api

import (
	"net/http"

	"github.com/peifengstudio/erminetq/internal/config"
)

// Handler wires the APIStore, SSE Broker, and server Config to HTTP routes.
type Handler struct {
	store  APIStore
	broker *Broker
	cfg    *config.Config
}

// NewHandler creates a Handler.
func NewHandler(store APIStore, broker *Broker, cfg *config.Config) *Handler {
	return &Handler{store: store, broker: broker, cfg: cfg}
}

// Register mounts all API routes onto mux.
func (h *Handler) Register(mux *http.ServeMux) {
	// Task CRUD + control
	mux.HandleFunc("GET /api/tasks", h.ListTasks)
	mux.HandleFunc("POST /api/tasks", h.CreateTask)
	mux.HandleFunc("GET /api/tasks/{id}", h.GetTask)
	mux.HandleFunc("POST /api/tasks/{id}/control", h.ControlTask)

	// Worker registry
	mux.HandleFunc("GET /api/workers", h.ListWorkers)
	mux.HandleFunc("POST /api/workers/register", h.RegisterWorker)

	// Pull-worker lifecycle (used by Python SDK)
	mux.HandleFunc("POST /api/worker/claim", h.ClaimTask)
	mux.HandleFunc("POST /api/worker/attempts/{id}/succeed", h.SucceedAttempt)
	mux.HandleFunc("POST /api/worker/attempts/{id}/fail", h.FailAttempt)

	// Schedules
	mux.HandleFunc("GET /api/schedules", h.ListSchedules)
	mux.HandleFunc("POST /api/schedules", h.CreateSchedule)
	mux.HandleFunc("PATCH /api/schedules/{id}", h.UpdateSchedule)
	mux.HandleFunc("POST /api/schedules/{id}/trigger", h.TriggerSchedule)

	// SSE
	mux.HandleFunc("GET /api/events", h.HandleEvents)
}
