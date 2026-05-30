package api

import (
	"net/http"

	"github.com/peifengstudio/erminetq/internal/store"
)

// registerWorkerRequest is the JSON body for POST /api/workers/register.
type registerWorkerRequest struct {
	Type        store.WorkerType `json:"type"`
	TaskTypes   []string         `json:"task_types"`
	Queue       string           `json:"queue"`
	Concurrency int              `json:"concurrency"`
	Socket      string           `json:"socket"` // Unix socket path; required for type=python
}

func (h *Handler) RegisterWorker(w http.ResponseWriter, r *http.Request) {
	var req registerWorkerRequest
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if req.Type == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "type is required"})
		return
	}

	in := store.CreateWorkerInput{
		Type:        req.Type,
		TaskTypes:   req.TaskTypes,
		Queue:       req.Queue,
		Concurrency: req.Concurrency,
	}
	if req.Socket != "" {
		in.SocketPath = &req.Socket
	}

	worker, err := h.store.CreateWorker(r.Context(), in)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, worker)
}

func (h *Handler) ListWorkers(w http.ResponseWriter, r *http.Request) {
	workers, err := h.store.ListWorkers(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	if workers == nil {
		workers = []*store.Worker{}
	}
	writeJSON(w, http.StatusOK, workers)
}
