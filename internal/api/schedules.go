package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/peifengstudio/erminetq/internal/store"
)

type createScheduleRequest struct {
	TaskType     string          `json:"task_type"`
	Queue        string          `json:"queue"`
	Payload      json.RawMessage `json:"payload"`
	CronExpr     *string         `json:"cron_expr"`
	IntervalSecs *int            `json:"interval_secs"`
	Enabled      bool            `json:"enabled"`
	NextRunAt    *time.Time      `json:"next_run_at"`
}

func (h *Handler) CreateSchedule(w http.ResponseWriter, r *http.Request) {
	var req createScheduleRequest
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if req.TaskType == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "task_type is required"})
		return
	}
	if req.CronExpr == nil && req.IntervalSecs == nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "one of cron_expr or interval_secs is required"})
		return
	}

	sc, err := h.store.CreateSchedule(r.Context(), store.CreateScheduleInput{
		TaskType:     req.TaskType,
		Queue:        req.Queue,
		Payload:      req.Payload,
		CronExpr:     req.CronExpr,
		IntervalSecs: req.IntervalSecs,
		Enabled:      req.Enabled,
		NextRunAt:    req.NextRunAt,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, sc)
}

func (h *Handler) ListSchedules(w http.ResponseWriter, r *http.Request) {
	schedules, err := h.store.ListSchedules(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	if schedules == nil {
		schedules = []*store.Schedule{}
	}
	writeJSON(w, http.StatusOK, schedules)
}

type updateScheduleRequest struct {
	Enabled      *bool      `json:"enabled"`
	CronExpr     *string    `json:"cron_expr"`
	IntervalSecs *int       `json:"interval_secs"`
	NextRunAt    *time.Time `json:"next_run_at"`
}

func (h *Handler) UpdateSchedule(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	var req updateScheduleRequest
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	sc, err := h.store.UpdateSchedule(r.Context(), id, store.UpdateScheduleInput{
		Enabled:      req.Enabled,
		CronExpr:     req.CronExpr,
		IntervalSecs: req.IntervalSecs,
		NextRunAt:    req.NextRunAt,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, sc)
}

// TriggerSchedule creates an immediate one-off task from a schedule's template.
func (h *Handler) TriggerSchedule(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	sc, err := h.store.GetSchedule(r.Context(), id)
	if err != nil {
		writeError(w, err)
		return
	}

	task, err := h.store.CreateTask(r.Context(), store.CreateTaskInput{
		Type:       sc.TaskType,
		Queue:      sc.Queue,
		Payload:    sc.Payload,
		ScheduleID: &sc.ID,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, task)
}
