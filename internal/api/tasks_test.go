package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/peifengstudio/erminetq/internal/config"
	"github.com/peifengstudio/erminetq/internal/store"
)

// ── mock store ────────────────────────────────────────────────────────────────

type mockStore struct {
	createTaskFn  func(context.Context, store.CreateTaskInput) (*store.Task, error)
	getTaskFn     func(context.Context, string) (*store.Task, []*store.Attempt, []*store.TaskEvent, error)
	listTasksFn   func(context.Context, store.ListTasksFilter) ([]*store.Task, error)
	haltTaskFn    func(context.Context, string) error
	resumeTaskFn  func(context.Context, string) error
	cancelTaskFn  func(context.Context, string) error
	retryTaskFn   func(context.Context, string) error
	restartTaskFn func(context.Context, string) (*store.Task, error)

	createWorkerFn func(context.Context, store.CreateWorkerInput) (*store.Worker, error)
	listWorkersFn  func(context.Context) ([]*store.Worker, error)

	createScheduleFn func(context.Context, store.CreateScheduleInput) (*store.Schedule, error)
	getScheduleFn    func(context.Context, string) (*store.Schedule, error)
	listSchedulesFn  func(context.Context) ([]*store.Schedule, error)
	updateScheduleFn func(context.Context, string, store.UpdateScheduleInput) (*store.Schedule, error)
}

func (m *mockStore) CreateTask(ctx context.Context, in store.CreateTaskInput) (*store.Task, error) {
	return m.createTaskFn(ctx, in)
}
func (m *mockStore) GetTask(ctx context.Context, id string) (*store.Task, []*store.Attempt, []*store.TaskEvent, error) {
	return m.getTaskFn(ctx, id)
}
func (m *mockStore) ListTasks(ctx context.Context, f store.ListTasksFilter) ([]*store.Task, error) {
	return m.listTasksFn(ctx, f)
}
func (m *mockStore) HaltTask(ctx context.Context, id string) error   { return m.haltTaskFn(ctx, id) }
func (m *mockStore) ResumeTask(ctx context.Context, id string) error { return m.resumeTaskFn(ctx, id) }
func (m *mockStore) CancelTask(ctx context.Context, id string) error { return m.cancelTaskFn(ctx, id) }
func (m *mockStore) RetryTask(ctx context.Context, id string) error  { return m.retryTaskFn(ctx, id) }
func (m *mockStore) RestartTask(ctx context.Context, id string) (*store.Task, error) {
	return m.restartTaskFn(ctx, id)
}
func (m *mockStore) CreateWorker(ctx context.Context, in store.CreateWorkerInput) (*store.Worker, error) {
	return m.createWorkerFn(ctx, in)
}
func (m *mockStore) ListWorkers(ctx context.Context) ([]*store.Worker, error) {
	return m.listWorkersFn(ctx)
}
func (m *mockStore) CreateSchedule(ctx context.Context, in store.CreateScheduleInput) (*store.Schedule, error) {
	return m.createScheduleFn(ctx, in)
}
func (m *mockStore) GetSchedule(ctx context.Context, id string) (*store.Schedule, error) {
	return m.getScheduleFn(ctx, id)
}
func (m *mockStore) ListSchedules(ctx context.Context) ([]*store.Schedule, error) {
	return m.listSchedulesFn(ctx)
}
func (m *mockStore) UpdateSchedule(ctx context.Context, id string, in store.UpdateScheduleInput) (*store.Schedule, error) {
	return m.updateScheduleFn(ctx, id, in)
}

// Pull-worker stubs — not exercised by existing task/schedule tests.
func (m *mockStore) ClaimTask(ctx context.Context, workerID, queue string, taskTypes []string, cfg *config.Config) (*store.Task, string, error) {
	return nil, "", nil
}
func (m *mockStore) SucceedAttempt(ctx context.Context, attemptID string, result json.RawMessage) error {
	return nil
}
func (m *mockStore) FailAttempt(ctx context.Context, attemptID, errMsg string) error {
	return nil
}

// ── helpers ───────────────────────────────────────────────────────────────────

func newTestHandler(ms *mockStore) *Handler {
	return NewHandler(ms, NewBroker(), &config.Config{})
}

func postJSON(t *testing.T, h http.Handler, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	b, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

func getReq(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

// ── POST /api/tasks ───────────────────────────────────────────────────────────

func TestCreateTask(t *testing.T) {
	okTask := &store.Task{ID: "t1", Type: "send_email", Status: store.TaskStatusQueued, CreatedAt: time.Now(), UpdatedAt: time.Now()}

	tests := []struct {
		name       string
		body       any
		storeFn    func(context.Context, store.CreateTaskInput) (*store.Task, error)
		wantStatus int
	}{
		{
			name:       "missing type",
			body:       map[string]any{"queue": "default"},
			storeFn:    nil,
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "ok",
			body: map[string]any{"type": "send_email"},
			storeFn: func(_ context.Context, in store.CreateTaskInput) (*store.Task, error) {
				if in.Type != "send_email" {
					t.Errorf("unexpected type %q", in.Type)
				}
				return okTask, nil
			},
			wantStatus: http.StatusCreated,
		},
		{
			name:       "store error",
			body:       map[string]any{"type": "send_email"},
			storeFn:    func(_ context.Context, _ store.CreateTaskInput) (*store.Task, error) { return nil, errors.New("boom") },
			wantStatus: http.StatusInternalServerError,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ms := &mockStore{createTaskFn: tc.storeFn}
			mux := http.NewServeMux()
			newTestHandler(ms).Register(mux)

			w := postJSON(t, mux, "/api/tasks", tc.body)
			if w.Code != tc.wantStatus {
				t.Errorf("got %d, want %d — body: %s", w.Code, tc.wantStatus, w.Body)
			}
		})
	}
}

// ── GET /api/tasks ────────────────────────────────────────────────────────────

func TestListTasks(t *testing.T) {
	tasks := []*store.Task{{ID: "t1", Type: "job"}}

	ms := &mockStore{
		listTasksFn: func(_ context.Context, f store.ListTasksFilter) ([]*store.Task, error) {
			if f.Status == nil || *f.Status != store.TaskStatusQueued {
				t.Errorf("expected status=queued filter, got %v", f.Status)
			}
			return tasks, nil
		},
	}
	mux := http.NewServeMux()
	newTestHandler(ms).Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/tasks?status=queued", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("got %d", w.Code)
	}
	var got []*store.Task
	if err := json.NewDecoder(w.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != "t1" {
		t.Errorf("unexpected tasks %v", got)
	}
}

func TestListTasksEmpty(t *testing.T) {
	ms := &mockStore{
		listTasksFn: func(_ context.Context, _ store.ListTasksFilter) ([]*store.Task, error) {
			return nil, nil
		},
	}
	mux := http.NewServeMux()
	newTestHandler(ms).Register(mux)

	w := getReq(t, mux, "/api/tasks")
	if w.Code != http.StatusOK {
		t.Fatalf("got %d", w.Code)
	}
	var got []any
	if err := json.NewDecoder(w.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("expected empty array, got %v", got)
	}
}

// ── POST /api/tasks/{id}/control ─────────────────────────────────────────────

func TestControlTask(t *testing.T) {
	errNotFound := store.ErrNotFound
	errBadTrans := store.ErrInvalidTransition

	newTask := &store.Task{ID: "t2", Type: "job", Status: store.TaskStatusQueued, CreatedAt: time.Now(), UpdatedAt: time.Now()}

	tests := []struct {
		name       string
		action     string
		ms         *mockStore
		wantStatus int
	}{
		{
			name:   "halt ok",
			action: "halt",
			ms: &mockStore{
				haltTaskFn: func(_ context.Context, _ string) error { return nil },
			},
			wantStatus: http.StatusOK,
		},
		{
			name:   "halt not found",
			action: "halt",
			ms: &mockStore{
				haltTaskFn: func(_ context.Context, _ string) error { return errNotFound },
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name:   "resume invalid transition",
			action: "resume",
			ms: &mockStore{
				resumeTaskFn: func(_ context.Context, _ string) error { return errBadTrans },
			},
			wantStatus: http.StatusConflict,
		},
		{
			name:   "cancel ok",
			action: "cancel",
			ms: &mockStore{
				cancelTaskFn: func(_ context.Context, _ string) error { return nil },
			},
			wantStatus: http.StatusOK,
		},
		{
			name:   "retry ok",
			action: "retry",
			ms: &mockStore{
				retryTaskFn: func(_ context.Context, _ string) error { return nil },
			},
			wantStatus: http.StatusOK,
		},
		{
			name:   "restart ok",
			action: "restart",
			ms: &mockStore{
				restartTaskFn: func(_ context.Context, _ string) (*store.Task, error) { return newTask, nil },
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "unknown action",
			action:     "vaporize",
			ms:         &mockStore{},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mux := http.NewServeMux()
			newTestHandler(tc.ms).Register(mux)

			w := postJSON(t, mux, "/api/tasks/t1/control", map[string]string{"action": tc.action})
			if w.Code != tc.wantStatus {
				t.Errorf("action=%q: got %d, want %d — body: %s", tc.action, w.Code, tc.wantStatus, w.Body)
			}
		})
	}
}
