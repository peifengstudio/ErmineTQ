package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/peifengstudio/erminetq/internal/store"
)

func TestRegisterWorker(t *testing.T) {
	okWorker := &store.Worker{
		ID:          "w1",
		Type:        store.WorkerTypeGo,
		TaskTypes:   []string{"send_email"},
		Queue:       "default",
		Concurrency: 4,
		Status:      store.WorkerStatusIdle,
		StartedAt:   time.Now(),
	}

	tests := []struct {
		name       string
		body       any
		storeFn    func(context.Context, store.CreateWorkerInput) (*store.Worker, error)
		wantStatus int
	}{
		{
			name:       "missing type",
			body:       map[string]any{"task_types": []string{"send_email"}},
			storeFn:    nil,
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "ok",
			body: map[string]any{
				"type":        "go",
				"task_types":  []string{"send_email"},
				"concurrency": 4,
			},
			storeFn: func(_ context.Context, in store.CreateWorkerInput) (*store.Worker, error) {
				if in.Type != store.WorkerTypeGo {
					t.Errorf("unexpected type %q", in.Type)
				}
				return okWorker, nil
			},
			wantStatus: http.StatusCreated,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ms := &mockStore{createWorkerFn: tc.storeFn}
			mux := http.NewServeMux()
			newTestHandler(ms).Register(mux)

			w := postJSON(t, mux, "/api/workers/register", tc.body)
			if w.Code != tc.wantStatus {
				t.Errorf("got %d, want %d — body: %s", w.Code, tc.wantStatus, w.Body)
			}
		})
	}
}

func TestListWorkers(t *testing.T) {
	workers := []*store.Worker{
		{ID: "w1", Type: store.WorkerTypeGo, TaskTypes: []string{}, Queue: "default", StartedAt: time.Now()},
	}

	ms := &mockStore{
		listWorkersFn: func(_ context.Context) ([]*store.Worker, error) { return workers, nil },
	}
	mux := http.NewServeMux()
	newTestHandler(ms).Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/workers", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("got %d", w.Code)
	}
	var got []*store.Worker
	if err := json.NewDecoder(w.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != "w1" {
		t.Errorf("unexpected workers %v", got)
	}
}

func TestListWorkersEmpty(t *testing.T) {
	ms := &mockStore{
		listWorkersFn: func(_ context.Context) ([]*store.Worker, error) { return nil, nil },
	}
	mux := http.NewServeMux()
	newTestHandler(ms).Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/workers", nil)
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
