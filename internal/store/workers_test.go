package store_test

import (
	"context"
	"testing"

	"github.com/peifengstudio/erminetq/internal/store"
)

func TestCreateWorker_FieldRoundTrip(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	in := store.CreateWorkerInput{
		Type:        store.WorkerTypeGo,
		TaskTypes:   []string{"crawl_url", "run_script"},
		Queue:       "priority",
		Concurrency: 4,
	}

	w, err := s.CreateWorker(ctx, in)
	if err != nil {
		t.Fatalf("CreateWorker: %v", err)
	}

	if w.ID == "" {
		t.Error("ID should be set")
	}
	if w.Type != store.WorkerTypeGo {
		t.Errorf("Type = %q, want %q", w.Type, store.WorkerTypeGo)
	}
	if len(w.TaskTypes) != 2 || w.TaskTypes[0] != "crawl_url" || w.TaskTypes[1] != "run_script" {
		t.Errorf("TaskTypes = %v, want [crawl_url run_script]", w.TaskTypes)
	}
	if w.Queue != "priority" {
		t.Errorf("Queue = %q, want %q", w.Queue, "priority")
	}
	if w.Concurrency != 4 {
		t.Errorf("Concurrency = %d, want 4", w.Concurrency)
	}
	if w.CurrentTaskCount != 0 {
		t.Errorf("CurrentTaskCount = %d, want 0", w.CurrentTaskCount)
	}
	if w.Status != store.WorkerStatusIdle {
		t.Errorf("Status = %q, want idle", w.Status)
	}
	if w.StartedAt.IsZero() {
		t.Error("StartedAt should not be zero")
	}
	if w.HeartbeatAt != nil {
		t.Errorf("HeartbeatAt = %v, want nil", w.HeartbeatAt)
	}
}

func TestCreateWorker_Defaults(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	w, err := s.CreateWorker(ctx, store.CreateWorkerInput{
		Type: store.WorkerTypePython,
	})
	if err != nil {
		t.Fatalf("CreateWorker: %v", err)
	}

	if w.Queue != "default" {
		t.Errorf("Queue = %q, want default", w.Queue)
	}
	if w.Concurrency != 1 {
		t.Errorf("Concurrency = %d, want 1", w.Concurrency)
	}
	if len(w.TaskTypes) != 0 {
		t.Errorf("TaskTypes = %v, want empty slice", w.TaskTypes)
	}
}

func TestCreateWorker_TaskTypesJSON(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	taskTypes := []string{"analyze_data", "train_model", "export_csv"}
	w, err := s.CreateWorker(ctx, store.CreateWorkerInput{
		Type:      store.WorkerTypePython,
		TaskTypes: taskTypes,
	})
	if err != nil {
		t.Fatalf("CreateWorker: %v", err)
	}

	got, err := s.GetWorker(ctx, w.ID)
	if err != nil {
		t.Fatalf("GetWorker: %v", err)
	}

	if len(got.TaskTypes) != len(taskTypes) {
		t.Fatalf("TaskTypes len=%d, want %d", len(got.TaskTypes), len(taskTypes))
	}
	for i, tt := range taskTypes {
		if got.TaskTypes[i] != tt {
			t.Errorf("TaskTypes[%d] = %q, want %q", i, got.TaskTypes[i], tt)
		}
	}
}

func TestGetWorker_RoundTrip(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	created, err := s.CreateWorker(ctx, store.CreateWorkerInput{
		Type:        store.WorkerTypeGo,
		TaskTypes:   []string{"job"},
		Queue:       "default",
		Concurrency: 2,
	})
	if err != nil {
		t.Fatalf("CreateWorker: %v", err)
	}

	got, err := s.GetWorker(ctx, created.ID)
	if err != nil {
		t.Fatalf("GetWorker: %v", err)
	}

	if got.ID != created.ID {
		t.Errorf("ID mismatch: %q vs %q", got.ID, created.ID)
	}
	if got.Type != created.Type {
		t.Errorf("Type mismatch: %q vs %q", got.Type, created.Type)
	}
	if got.Concurrency != created.Concurrency {
		t.Errorf("Concurrency mismatch: %d vs %d", got.Concurrency, created.Concurrency)
	}
}

func TestGetWorker_NotFound(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	_, err := s.GetWorker(ctx, "nonexistent-worker")
	if err != store.ErrNotFound {
		t.Errorf("GetWorker = %v, want ErrNotFound", err)
	}
}

func TestListWorkers_Empty(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	workers, err := s.ListWorkers(ctx)
	if err != nil {
		t.Fatalf("ListWorkers: %v", err)
	}
	if workers != nil {
		t.Errorf("expected nil slice for empty result, got %v", workers)
	}
}

func TestListWorkers_MultipleWorkers(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if _, err := s.CreateWorker(ctx, store.CreateWorkerInput{
			Type: store.WorkerTypeGo,
		}); err != nil {
			t.Fatalf("CreateWorker: %v", err)
		}
	}

	workers, err := s.ListWorkers(ctx)
	if err != nil {
		t.Fatalf("ListWorkers: %v", err)
	}
	if len(workers) != 3 {
		t.Errorf("len(workers) = %d, want 3", len(workers))
	}
}

// ── socket_path (migration 003) ───────────────────────────────────────────────

func TestCreateWorker_WithSocketPath(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	sock := "/tmp/erminetq_bridge.sock"
	w, err := s.CreateWorker(ctx, store.CreateWorkerInput{
		Type:        store.WorkerTypePython,
		TaskTypes:   []string{"analyze_data", "train_model"},
		Queue:       "data",
		Concurrency: 2,
		SocketPath:  &sock,
	})
	if err != nil {
		t.Fatalf("CreateWorker: %v", err)
	}
	if w.SocketPath == nil || *w.SocketPath != sock {
		t.Errorf("SocketPath = %v, want %q", w.SocketPath, sock)
	}

	// Round-trip through GetWorker.
	got, err := s.GetWorker(ctx, w.ID)
	if err != nil {
		t.Fatalf("GetWorker: %v", err)
	}
	if got.SocketPath == nil || *got.SocketPath != sock {
		t.Errorf("GetWorker SocketPath = %v, want %q", got.SocketPath, sock)
	}
}

func TestCreateWorker_NilSocketPath(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	w, err := s.CreateWorker(ctx, store.CreateWorkerInput{
		Type:      store.WorkerTypeGo,
		TaskTypes: []string{"send_email"},
		// SocketPath intentionally omitted (nil)
	})
	if err != nil {
		t.Fatalf("CreateWorker: %v", err)
	}
	if w.SocketPath != nil {
		t.Errorf("SocketPath should be nil for Go worker, got %q", *w.SocketPath)
	}

	got, err := s.GetWorker(ctx, w.ID)
	if err != nil {
		t.Fatalf("GetWorker: %v", err)
	}
	if got.SocketPath != nil {
		t.Errorf("SocketPath should be nil after round-trip, got %q", *got.SocketPath)
	}
}

func TestListWorkers_SocketPathPreserved(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	sock := "/tmp/bridge.sock"
	if _, err := s.CreateWorker(ctx, store.CreateWorkerInput{
		Type: store.WorkerTypePython, TaskTypes: []string{"job"}, SocketPath: &sock,
	}); err != nil {
		t.Fatalf("CreateWorker python: %v", err)
	}
	if _, err := s.CreateWorker(ctx, store.CreateWorkerInput{
		Type: store.WorkerTypeGo, TaskTypes: []string{"other"},
	}); err != nil {
		t.Fatalf("CreateWorker go: %v", err)
	}

	workers, err := s.ListWorkers(ctx)
	if err != nil {
		t.Fatalf("ListWorkers: %v", err)
	}

	var found bool
	for _, w := range workers {
		if w.Type == store.WorkerTypePython {
			if w.SocketPath == nil || *w.SocketPath != sock {
				t.Errorf("python worker SocketPath = %v, want %q", w.SocketPath, sock)
			}
			found = true
		} else {
			if w.SocketPath != nil {
				t.Errorf("go worker SocketPath should be nil, got %q", *w.SocketPath)
			}
		}
	}
	if !found {
		t.Error("python worker not found in list")
	}
}
