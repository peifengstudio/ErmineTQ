package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/peifengstudio/erminetq/internal/store"
)

// createTaskRequest is the JSON body for POST /api/tasks.
type createTaskRequest struct {
	Type        string          `json:"type"`
	Queue       string          `json:"queue"`
	Payload     json.RawMessage `json:"payload"`
	Priority    int             `json:"priority"`
	MaxRetries  int             `json:"max_retries"`
	TimeoutSecs *int            `json:"timeout_secs"`
	RunAt       *time.Time      `json:"run_at"`
}

func (h *Handler) CreateTask(w http.ResponseWriter, r *http.Request) {
	var req createTaskRequest
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if req.Type == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "type is required"})
		return
	}

	in := store.CreateTaskInput{
		Type:        req.Type,
		Queue:       req.Queue,
		Payload:     req.Payload,
		Priority:    req.Priority,
		MaxRetries:  req.MaxRetries,
		TimeoutSecs: req.TimeoutSecs,
	}
	if req.RunAt != nil {
		in.RunAt = *req.RunAt
	}

	task, err := h.store.CreateTask(r.Context(), in)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, task)
}

func (h *Handler) GetTask(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	task, attempts, events, err := h.store.GetTask(r.Context(), id)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"task":     task,
		"attempts": attempts,
		"events":   events,
	})
}

func (h *Handler) ListTasks(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := store.ListTasksFilter{}

	if s := q.Get("status"); s != "" {
		ts := store.TaskStatus(s)
		f.Status = &ts
	}
	if t := q.Get("type"); t != "" {
		f.Type = &t
	}
	if qu := q.Get("queue"); qu != "" {
		f.Queue = &qu
	}
	if lim := q.Get("limit"); lim != "" {
		if n, err := strconv.Atoi(lim); err == nil {
			f.Limit = n
		}
	}
	if off := q.Get("offset"); off != "" {
		if n, err := strconv.Atoi(off); err == nil {
			f.Offset = n
		}
	}

	tasks, err := h.store.ListTasks(r.Context(), f)
	if err != nil {
		writeError(w, err)
		return
	}
	if tasks == nil {
		tasks = []*store.Task{}
	}
	writeJSON(w, http.StatusOK, tasks)
}

// controlRequest is the JSON body for POST /api/tasks/{id}/control.
type controlRequest struct {
	Action string `json:"action"`
}

func (h *Handler) ControlTask(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	var req controlRequest
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	ctx := r.Context()
	switch req.Action {
	case "halt":
		if err := h.store.HaltTask(ctx, id); err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})

	case "resume":
		if err := h.store.ResumeTask(ctx, id); err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})

	case "cancel":
		if err := h.store.CancelTask(ctx, id); err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})

	case "retry":
		if err := h.store.RetryTask(ctx, id); err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})

	case "restart":
		newTask, err := h.store.RestartTask(ctx, id)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, newTask)

	default:
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "unknown action: " + req.Action})
	}
}
