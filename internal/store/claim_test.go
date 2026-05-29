package store_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/peifengstudio/erminetq/internal/config"
	"github.com/peifengstudio/erminetq/internal/store"
)

// ── helpers ───────────────────────────────────────────────────────────────────

// unlimitedCfg returns a config with no effective limits (global = 10 000).
func unlimitedCfg() *config.Config {
	return &config.Config{
		Limits:    config.LimitsConfig{Global: 10_000},
		Queues:    map[string]config.QueueConfig{},
		TaskTypes: map[string]config.TaskTypeConfig{},
	}
}

// cfgWithGlobal returns a config with the given global limit only.
func cfgWithGlobal(n int) *config.Config {
	return &config.Config{
		Limits:    config.LimitsConfig{Global: n},
		Queues:    map[string]config.QueueConfig{},
		TaskTypes: map[string]config.TaskTypeConfig{},
	}
}

// cfgWithQueueLimit returns a config with a per-queue limit and a high global.
func cfgWithQueueLimit(queue string, n int) *config.Config {
	return &config.Config{
		Limits: config.LimitsConfig{Global: 10_000},
		Queues: map[string]config.QueueConfig{
			queue: {Limit: n},
		},
		TaskTypes: map[string]config.TaskTypeConfig{},
	}
}

// cfgWithTypeLimit returns a config with a per-type limit and a high global.
func cfgWithTypeLimit(taskType string, n int) *config.Config {
	return &config.Config{
		Limits: config.LimitsConfig{Global: 10_000},
		Queues: map[string]config.QueueConfig{},
		TaskTypes: map[string]config.TaskTypeConfig{
			taskType: {Limit: n},
		},
	}
}

// createWorker creates a worker with the given taskTypes and concurrency=10.
func createWorker(t *testing.T, s *store.Store, taskTypes []string) *store.Worker {
	t.Helper()
	w, err := s.CreateWorker(context.Background(), store.CreateWorkerInput{
		Type:        store.WorkerTypeGo,
		TaskTypes:   taskTypes,
		Queue:       "default",
		Concurrency: 10,
	})
	if err != nil {
		t.Fatalf("CreateWorker: %v", err)
	}
	return w
}

// createTask creates a queued task with the given type in the default queue.
func createTask(t *testing.T, s *store.Store, taskType string) *store.Task {
	t.Helper()
	task, err := s.CreateTask(context.Background(), store.CreateTaskInput{Type: taskType})
	if err != nil {
		t.Fatalf("CreateTask(%s): %v", taskType, err)
	}
	return task
}

// claim calls ClaimTask and fails the test on error.
func claim(t *testing.T, s *store.Store, workerID string, taskTypes []string, cfg *config.Config) *store.Task {
	t.Helper()
	ctx := context.Background()
	got, _, err := s.ClaimTask(ctx, workerID, "default", taskTypes, cfg)
	if err != nil {
		t.Fatalf("ClaimTask: %v", err)
	}
	return got
}

// ── tests ─────────────────────────────────────────────────────────────────────

func TestClaimTask_NoTasksQueued(t *testing.T) {
	s := openTestStore(t)
	w := createWorker(t, s, []string{"job"})

	got := claim(t, s, w.ID, []string{"job"}, unlimitedCfg())
	if got != nil {
		t.Errorf("expected nil, got task %q", got.ID)
	}
}

func TestClaimTask_EmptyTypeList(t *testing.T) {
	s := openTestStore(t)
	createTask(t, s, "job")
	w := createWorker(t, s, []string{"job"})

	got := claim(t, s, w.ID, []string{}, unlimitedCfg())
	if got != nil {
		t.Errorf("expected nil for empty taskTypes, got %q", got.ID)
	}
}

func TestClaimTask_Success(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	task := createTask(t, s, "crawl")
	w := createWorker(t, s, []string{"crawl"})

	got := claim(t, s, w.ID, []string{"crawl"}, unlimitedCfg())
	if got == nil {
		t.Fatal("expected a task, got nil")
	}
	if got.ID != task.ID {
		t.Errorf("ID = %q, want %q", got.ID, task.ID)
	}
	if got.Status != store.TaskStatusRunning {
		t.Errorf("Status = %q, want running", got.Status)
	}
	if got.Type != "crawl" {
		t.Errorf("Type = %q, want crawl", got.Type)
	}

	// Verify the task is running in the DB.
	fresh, _, _, err := s.GetTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if fresh.Status != store.TaskStatusRunning {
		t.Errorf("persisted status = %q, want running", fresh.Status)
	}
}

func TestClaimTask_SideEffects(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	task := createTask(t, s, "job")
	w := createWorker(t, s, []string{"job"})

	got := claim(t, s, w.ID, []string{"job"}, unlimitedCfg())
	if got == nil {
		t.Fatal("expected a task, got nil")
	}

	// ── Attempt ───────────────────────────────────────────────────────────────
	_, attempts, events, err := s.GetTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if len(attempts) != 1 {
		t.Fatalf("len(attempts) = %d, want 1", len(attempts))
	}
	a := attempts[0]
	if a.AttemptNum != 1 {
		t.Errorf("AttemptNum = %d, want 1", a.AttemptNum)
	}
	if a.WorkerID == nil || *a.WorkerID != w.ID {
		t.Errorf("WorkerID = %v, want %q", a.WorkerID, w.ID)
	}
	if a.Status != store.AttemptStatusRunning {
		t.Errorf("attempt Status = %q, want running", a.Status)
	}
	if a.StartedAt == nil {
		t.Error("StartedAt should not be nil")
	}

	// ── Events (queued + started) ─────────────────────────────────────────────
	if len(events) != 2 {
		t.Fatalf("len(events) = %d, want 2 (queued + started)", len(events))
	}
	if events[0].Event != store.TaskEventQueued {
		t.Errorf("events[0] = %q, want queued", events[0].Event)
	}
	startedEv := events[1]
	if startedEv.Event != store.TaskEventStarted {
		t.Errorf("events[1] = %q, want started", startedEv.Event)
	}
	// Detail must contain attempt_id matching the attempt we found.
	var detail map[string]string
	if err := json.Unmarshal(startedEv.Detail, &detail); err != nil {
		t.Fatalf("unmarshal started detail: %v", err)
	}
	if detail["attempt_id"] != a.ID {
		t.Errorf("detail.attempt_id = %q, want %q", detail["attempt_id"], a.ID)
	}

	// ── Worker count ──────────────────────────────────────────────────────────
	worker, err := s.GetWorker(ctx, w.ID)
	if err != nil {
		t.Fatalf("GetWorker: %v", err)
	}
	if worker.CurrentTaskCount != 1 {
		t.Errorf("CurrentTaskCount = %d, want 1", worker.CurrentTaskCount)
	}
}

func TestClaimTask_WorkerBecomesFullAtConcurrencyLimit(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	// Worker with concurrency=2.
	w, err := s.CreateWorker(ctx, store.CreateWorkerInput{
		Type:        store.WorkerTypeGo,
		TaskTypes:   []string{"job"},
		Queue:       "default",
		Concurrency: 2,
	})
	if err != nil {
		t.Fatalf("CreateWorker: %v", err)
	}

	createTask(t, s, "job")
	createTask(t, s, "job")

	// First claim: count=1, concurrency=2 → still idle.
	claim(t, s, w.ID, []string{"job"}, unlimitedCfg())
	after1, err := s.GetWorker(ctx, w.ID)
	if err != nil {
		t.Fatalf("GetWorker: %v", err)
	}
	if after1.Status != store.WorkerStatusIdle {
		t.Errorf("after 1 claim: status = %q, want idle", after1.Status)
	}

	// Second claim: count=2, concurrency=2 → busy.
	claim(t, s, w.ID, []string{"job"}, unlimitedCfg())
	after2, err := s.GetWorker(ctx, w.ID)
	if err != nil {
		t.Fatalf("GetWorker: %v", err)
	}
	if after2.Status != store.WorkerStatusBusy {
		t.Errorf("after 2 claims: status = %q, want busy", after2.Status)
	}
	if after2.CurrentTaskCount != 2 {
		t.Errorf("CurrentTaskCount = %d, want 2", after2.CurrentTaskCount)
	}
}

func TestClaimTask_GlobalLimitBlock(t *testing.T) {
	s := openTestStore(t)
	createTask(t, s, "job")
	createTask(t, s, "job")
	w := createWorker(t, s, []string{"job"})
	cfg := cfgWithGlobal(1)

	// First claim succeeds: 0 running → 0 < 1.
	got1 := claim(t, s, w.ID, []string{"job"}, cfg)
	if got1 == nil {
		t.Fatal("first claim should succeed")
	}

	// Second claim blocked: 1 running ≥ global limit 1.
	got2 := claim(t, s, w.ID, []string{"job"}, cfg)
	if got2 != nil {
		t.Errorf("second claim should be blocked by global limit, got %q", got2.ID)
	}
}

func TestClaimTask_QueueLimitBlock(t *testing.T) {
	s := openTestStore(t)
	createTask(t, s, "job")
	createTask(t, s, "job")
	w := createWorker(t, s, []string{"job"})
	cfg := cfgWithQueueLimit("default", 1)

	got1 := claim(t, s, w.ID, []string{"job"}, cfg)
	if got1 == nil {
		t.Fatal("first claim should succeed")
	}

	got2 := claim(t, s, w.ID, []string{"job"}, cfg)
	if got2 != nil {
		t.Errorf("second claim should be blocked by queue limit, got %q", got2.ID)
	}
}

func TestClaimTask_TypeLimitBlock(t *testing.T) {
	s := openTestStore(t)
	createTask(t, s, "crawl")
	createTask(t, s, "crawl")
	w := createWorker(t, s, []string{"crawl"})
	cfg := cfgWithTypeLimit("crawl", 1)

	got1 := claim(t, s, w.ID, []string{"crawl"}, cfg)
	if got1 == nil {
		t.Fatal("first claim should succeed")
	}

	got2 := claim(t, s, w.ID, []string{"crawl"}, cfg)
	if got2 != nil {
		t.Errorf("second crawl claim should be blocked by type limit, got %q", got2.ID)
	}
}

func TestClaimTask_TypeLimitFiltersPartially(t *testing.T) {
	// "crawl" is at limit, but "index" still has capacity.
	// ClaimTask(["crawl","index"]) should skip crawl and claim the index task.
	s := openTestStore(t)
	createTask(t, s, "crawl")
	indexTask := createTask(t, s, "index")
	w := createWorker(t, s, []string{"crawl", "index"})
	cfg := cfgWithTypeLimit("crawl", 1)

	// Fill crawl limit.
	got1 := claim(t, s, w.ID, []string{"crawl", "index"}, cfg)
	if got1 == nil {
		t.Fatal("first claim should succeed")
	}

	// Now crawl is full; should claim the index task.
	got2 := claim(t, s, w.ID, []string{"crawl", "index"}, cfg)
	if got2 == nil {
		t.Fatal("second claim should succeed (index type still has capacity)")
	}
	if got2.ID != indexTask.ID {
		t.Errorf("claimed %q, want index task %q", got2.ID, indexTask.ID)
	}
	if got2.Type != "index" {
		t.Errorf("claimed type = %q, want index", got2.Type)
	}
}

func TestClaimTask_FutureRunAt(t *testing.T) {
	s := openTestStore(t)
	w := createWorker(t, s, []string{"job"})

	// Task scheduled 1 hour from now.
	_, err := s.CreateTask(context.Background(), store.CreateTaskInput{
		Type:  "job",
		RunAt: time.Now().UTC().Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	got := claim(t, s, w.ID, []string{"job"}, unlimitedCfg())
	if got != nil {
		t.Errorf("future-run_at task should not be claimable, got %q", got.ID)
	}
}

func TestClaimTask_QueueMismatch(t *testing.T) {
	s := openTestStore(t)
	// Task in queue "batch"; claim targets queue "default".
	_, err := s.CreateTask(context.Background(), store.CreateTaskInput{
		Type:  "job",
		Queue: "batch",
	})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	w := createWorker(t, s, []string{"job"})

	got := claim(t, s, w.ID, []string{"job"}, unlimitedCfg())
	if got != nil {
		t.Errorf("wrong-queue task should not be claimable, got %q", got.ID)
	}
}

func TestClaimTask_TypeMismatch(t *testing.T) {
	s := openTestStore(t)
	createTask(t, s, "crawl")
	w := createWorker(t, s, []string{"index"})

	// Worker only handles "index", but only "crawl" is queued.
	got := claim(t, s, w.ID, []string{"index"}, unlimitedCfg())
	if got != nil {
		t.Errorf("wrong-type task should not be claimable, got %q", got.ID)
	}
}

func TestClaimTask_PriorityOrdering(t *testing.T) {
	s := openTestStore(t)
	w := createWorker(t, s, []string{"job"})

	lowTask, _ := s.CreateTask(context.Background(), store.CreateTaskInput{Type: "job", Priority: 1})
	highTask, _ := s.CreateTask(context.Background(), store.CreateTaskInput{Type: "job", Priority: 99})

	got := claim(t, s, w.ID, []string{"job"}, unlimitedCfg())
	if got == nil {
		t.Fatal("expected a task")
	}
	if got.ID != highTask.ID {
		t.Errorf("claimed %q (priority=%d), want high-priority task %q",
			got.ID, got.Priority, highTask.ID)
	}
	_ = lowTask
}

func TestClaimTask_AttemptNumIncrementsOnRetry(t *testing.T) {
	// First claim → attempt_num=1. We verify the count query works for the
	// base case. Subsequent increments are tested with FailAttempt (Phase 1c).
	s := openTestStore(t)
	task := createTask(t, s, "job")
	w := createWorker(t, s, []string{"job"})

	claim(t, s, w.ID, []string{"job"}, unlimitedCfg())

	_, attempts, _, err := s.GetTask(context.Background(), task.ID)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if len(attempts) != 1 {
		t.Fatalf("len(attempts) = %d, want 1", len(attempts))
	}
	if attempts[0].AttemptNum != 1 {
		t.Errorf("AttemptNum = %d, want 1", attempts[0].AttemptNum)
	}
}
