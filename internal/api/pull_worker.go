package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/peifengstudio/erminetq/internal/store"
)

// claimRequest is the JSON body for POST /api/worker/claim.
type claimRequest struct {
	WorkerID  string   `json:"worker_id"`
	TaskTypes []string `json:"task_types"`
	Queue     string   `json:"queue"`
}

// claimResponse is returned when a task is successfully claimed.
type claimResponse struct {
	Task      *store.Task `json:"task"`
	AttemptID string      `json:"attempt_id"`
}

// ClaimTask handles POST /api/worker/claim.
// Returns 200 + task when a task was claimed, 204 when the queue is empty.
func (h *Handler) ClaimTask(w http.ResponseWriter, r *http.Request) {
	var req claimRequest
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if req.WorkerID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "worker_id is required"})
		return
	}
	if len(req.TaskTypes) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "task_types must not be empty"})
		return
	}

	queue := req.Queue
	if queue == "" {
		queue = "default"
	}

	task, attemptID, err := h.store.ClaimTask(r.Context(), req.WorkerID, queue, req.TaskTypes, h.cfg)
	if err != nil {
		writeError(w, err)
		return
	}
	if task == nil {
		// Queue is empty — SDK will back off and retry.
		w.WriteHeader(http.StatusNoContent)
		return
	}
	writeJSON(w, http.StatusOK, claimResponse{Task: task, AttemptID: attemptID})
}

// succeedRequest is the JSON body for POST /api/worker/attempts/{id}/succeed.
type succeedRequest struct {
	Result json.RawMessage `json:"result"`
}

// SucceedAttempt handles POST /api/worker/attempts/{id}/succeed.
func (h *Handler) SucceedAttempt(w http.ResponseWriter, r *http.Request) {
	attemptID := r.PathValue("id")

	var req succeedRequest
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	// Treat omitted result as JSON null so store always gets valid JSON.
	if req.Result == nil {
		req.Result = json.RawMessage("null")
	}

	if err := h.store.SucceedAttempt(r.Context(), attemptID, req.Result); err != nil {
		writeAttemptError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// failRequest is the JSON body for POST /api/worker/attempts/{id}/fail.
type failRequest struct {
	Error string `json:"error"`
}

// FailAttempt handles POST /api/worker/attempts/{id}/fail.
func (h *Handler) FailAttempt(w http.ResponseWriter, r *http.Request) {
	attemptID := r.PathValue("id")

	var req failRequest
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if req.Error == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "error message is required"})
		return
	}

	if err := h.store.FailAttempt(r.Context(), attemptID, req.Error); err != nil {
		writeAttemptError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// writeAttemptError maps attempt-specific sentinel errors to HTTP status codes.
func writeAttemptError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
	case errors.Is(err, store.ErrAttemptNotRunning):
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
	default:
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
}
