package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/peifengstudio/erminetq/internal/store"
)

// patchJSON sends a PATCH request with a JSON body.
func patchJSON(t *testing.T, h http.Handler, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	b, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPatch, path, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

func TestCreateSchedule(t *testing.T) {
	interval := 3600
	okSched := &store.Schedule{ID: "s1", TaskType: "hourly_job", Queue: "default", Enabled: true, CreatedAt: time.Now()}

	tests := []struct {
		name       string
		body       any
		storeFn    func(context.Context, store.CreateScheduleInput) (*store.Schedule, error)
		wantStatus int
	}{
		{
			name:       "missing task_type",
			body:       map[string]any{"interval_secs": 3600},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing schedule expression",
			body:       map[string]any{"task_type": "hourly_job"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "ok with interval",
			body: map[string]any{"task_type": "hourly_job", "interval_secs": 3600, "enabled": true},
			storeFn: func(_ context.Context, in store.CreateScheduleInput) (*store.Schedule, error) {
				if in.TaskType != "hourly_job" {
					t.Errorf("unexpected task_type %q", in.TaskType)
				}
				if in.IntervalSecs == nil || *in.IntervalSecs != interval {
					t.Errorf("unexpected interval_secs")
				}
				return okSched, nil
			},
			wantStatus: http.StatusCreated,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ms := &mockStore{createScheduleFn: tc.storeFn}
			mux := http.NewServeMux()
			newTestHandler(ms).Register(mux)

			w := postJSON(t, mux, "/api/schedules", tc.body)
			if w.Code != tc.wantStatus {
				t.Errorf("got %d, want %d — body: %s", w.Code, tc.wantStatus, w.Body)
			}
		})
	}
}

func TestUpdateSchedule(t *testing.T) {
	okSched := &store.Schedule{ID: "s1", TaskType: "job", Enabled: true, CreatedAt: time.Now()}

	tests := []struct {
		name       string
		body       any
		storeFn    func(context.Context, string, store.UpdateScheduleInput) (*store.Schedule, error)
		wantStatus int
	}{
		{
			name: "enable",
			body: map[string]any{"enabled": true},
			storeFn: func(_ context.Context, id string, in store.UpdateScheduleInput) (*store.Schedule, error) {
				if id != "s1" {
					t.Errorf("wrong id %q", id)
				}
				if in.Enabled == nil || !*in.Enabled {
					t.Errorf("expected enabled=true")
				}
				return okSched, nil
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "disable",
			body: map[string]any{"enabled": false},
			storeFn: func(_ context.Context, _ string, in store.UpdateScheduleInput) (*store.Schedule, error) {
				if in.Enabled == nil || *in.Enabled {
					t.Errorf("expected enabled=false")
				}
				return okSched, nil
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "not found",
			body: map[string]any{"enabled": true},
			storeFn: func(_ context.Context, _ string, _ store.UpdateScheduleInput) (*store.Schedule, error) {
				return nil, store.ErrNotFound
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ms := &mockStore{updateScheduleFn: tc.storeFn}
			mux := http.NewServeMux()
			newTestHandler(ms).Register(mux)

			w := patchJSON(t, mux, "/api/schedules/s1", tc.body)
			if w.Code != tc.wantStatus {
				t.Errorf("got %d, want %d — body: %s", w.Code, tc.wantStatus, w.Body)
			}
		})
	}
}

func TestTriggerSchedule(t *testing.T) {
	sc := &store.Schedule{ID: "s1", TaskType: "nightly", Queue: "default", CreatedAt: time.Now()}
	createdTask := &store.Task{ID: "t1", Type: "nightly", Queue: "default", Status: store.TaskStatusQueued, CreatedAt: time.Now(), UpdatedAt: time.Now()}

	ms := &mockStore{
		getScheduleFn: func(_ context.Context, id string) (*store.Schedule, error) {
			if id != "s1" {
				t.Errorf("unexpected id %q", id)
			}
			return sc, nil
		},
		createTaskFn: func(_ context.Context, in store.CreateTaskInput) (*store.Task, error) {
			if in.Type != "nightly" {
				t.Errorf("unexpected task type %q", in.Type)
			}
			if in.ScheduleID == nil || *in.ScheduleID != "s1" {
				t.Errorf("expected schedule_id=s1")
			}
			return createdTask, nil
		},
	}
	mux := http.NewServeMux()
	newTestHandler(ms).Register(mux)

	w := postJSON(t, mux, "/api/schedules/s1/trigger", map[string]any{})
	if w.Code != http.StatusCreated {
		t.Fatalf("got %d — body: %s", w.Code, w.Body)
	}
	var got store.Task
	if err := json.NewDecoder(w.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.ID != "t1" {
		t.Errorf("unexpected task id %q", got.ID)
	}
}

func TestTriggerScheduleNotFound(t *testing.T) {
	ms := &mockStore{
		getScheduleFn: func(_ context.Context, _ string) (*store.Schedule, error) {
			return nil, store.ErrNotFound
		},
	}
	mux := http.NewServeMux()
	newTestHandler(ms).Register(mux)

	w := postJSON(t, mux, "/api/schedules/missing/trigger", map[string]any{})
	if w.Code != http.StatusNotFound {
		t.Errorf("got %d, want 404", w.Code)
	}
}

func TestListSchedulesEmpty(t *testing.T) {
	ms := &mockStore{
		listSchedulesFn: func(_ context.Context) ([]*store.Schedule, error) { return nil, nil },
	}
	mux := http.NewServeMux()
	newTestHandler(ms).Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/schedules", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("got %d", w.Code)
	}
	var got []any
	if err := json.NewDecoder(w.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("expected empty array")
	}
}
