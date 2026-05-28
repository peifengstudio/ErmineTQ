package store_test

import (
	"context"
	"errors"
	"testing"

	"github.com/peifengstudio/erminetq/internal/store"
)

// forceTaskStatus bypasses the Store layer to put a task into any raw status.
// Used to set up terminal or intermediate states that can't be reached through
// a short API call sequence (e.g. dead, cancelled with non-zero retry_count).
func forceTaskStatus(t *testing.T, s *store.Store, taskID string, status store.TaskStatus) {
	t.Helper()
	if _, err := s.DB().Exec(
		`UPDATE tasks SET status = ? WHERE id = ?`, string(status), taskID,
	); err != nil {
		t.Fatalf("forceTaskStatus(%s): %v", status, err)
	}
}

// forceRetryCount sets retry_count directly for testing RetryTask's reset behaviour.
func forceRetryCount(t *testing.T, s *store.Store, taskID string, n int) {
	t.Helper()
	if _, err := s.DB().Exec(
		`UPDATE tasks SET retry_count = ? WHERE id = ?`, n, taskID,
	); err != nil {
		t.Fatalf("forceRetryCount(%d): %v", n, err)
	}
}

// haltedTask creates a running task and halts it, returning the task ID.
func haltedTask(t *testing.T, s *store.Store) string {
	t.Helper()
	ctx := context.Background()
	task, err := s.CreateTask(ctx, store.CreateTaskInput{Type: "job"})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	w := createWorker(t, s, []string{"job"})
	claim(t, s, w.ID, []string{"job"}, unlimitedCfg())
	if err := s.HaltTask(ctx, task.ID); err != nil {
		t.Fatalf("HaltTask: %v", err)
	}
	return task.ID
}

// ── HaltTask ──────────────────────────────────────────────────────────────────

func TestHaltTask_Success(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	task, _ := claimFirst(t, s, "job", 3)

	if err := s.HaltTask(ctx, task.ID); err != nil {
		t.Fatalf("HaltTask: %v", err)
	}

	fresh, _, _, err := s.GetTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if fresh.Status != store.TaskStatusHalted {
		t.Errorf("status = %q, want halted", fresh.Status)
	}

	ev := lastEvent(t, s, task.ID)
	if ev.Event != store.TaskEventHalted {
		t.Errorf("last event = %q, want halted", ev.Event)
	}
}

func TestHaltTask_NotFound(t *testing.T) {
	s := openTestStore(t)
	err := s.HaltTask(context.Background(), "no-such-id")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}

func TestHaltTask_InvalidState_Queued(t *testing.T) {
	s := openTestStore(t)
	task := createTask(t, s, "job") // queued, not running

	err := s.HaltTask(context.Background(), task.ID)
	if !errors.Is(err, store.ErrInvalidTransition) {
		t.Errorf("err = %v, want ErrInvalidTransition", err)
	}
}

// ── ResumeTask ────────────────────────────────────────────────────────────────

func TestResumeTask_Success(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	id := haltedTask(t, s)

	if err := s.ResumeTask(ctx, id); err != nil {
		t.Fatalf("ResumeTask: %v", err)
	}

	fresh, _, _, err := s.GetTask(ctx, id)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if fresh.Status != store.TaskStatusQueued {
		t.Errorf("status = %q, want queued", fresh.Status)
	}

	ev := lastEvent(t, s, id)
	if ev.Event != store.TaskEventQueued {
		t.Errorf("last event = %q, want queued", ev.Event)
	}
}

func TestResumeTask_IsClaimableAfterResume(t *testing.T) {
	s := openTestStore(t)
	id := haltedTask(t, s)

	if err := s.ResumeTask(context.Background(), id); err != nil {
		t.Fatalf("ResumeTask: %v", err)
	}

	// A new worker should be able to claim it immediately.
	w := createWorker(t, s, []string{"job"})
	got := claim(t, s, w.ID, []string{"job"}, unlimitedCfg())
	if got == nil {
		t.Fatal("expected resumed task to be claimable, got nil")
	}
	if got.ID != id {
		t.Errorf("claimed %q, want %q", got.ID, id)
	}
}

func TestResumeTask_NotFound(t *testing.T) {
	s := openTestStore(t)
	err := s.ResumeTask(context.Background(), "no-such-id")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}

func TestResumeTask_InvalidState_Queued(t *testing.T) {
	s := openTestStore(t)
	task := createTask(t, s, "job")

	err := s.ResumeTask(context.Background(), task.ID)
	if !errors.Is(err, store.ErrInvalidTransition) {
		t.Errorf("err = %v, want ErrInvalidTransition", err)
	}
}

// ── CancelTask ────────────────────────────────────────────────────────────────

func TestCancelTask_FromQueued(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	task := createTask(t, s, "job")

	if err := s.CancelTask(ctx, task.ID); err != nil {
		t.Fatalf("CancelTask: %v", err)
	}

	fresh, _, _, err := s.GetTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if fresh.Status != store.TaskStatusCancelled {
		t.Errorf("status = %q, want cancelled", fresh.Status)
	}

	ev := lastEvent(t, s, task.ID)
	if ev.Event != store.TaskEventCancelled {
		t.Errorf("last event = %q, want cancelled", ev.Event)
	}
}

func TestCancelTask_FromHalted(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	id := haltedTask(t, s)

	if err := s.CancelTask(ctx, id); err != nil {
		t.Fatalf("CancelTask from halted: %v", err)
	}

	fresh, _, _, err := s.GetTask(ctx, id)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if fresh.Status != store.TaskStatusCancelled {
		t.Errorf("status = %q, want cancelled", fresh.Status)
	}
}

func TestCancelTask_NotFound(t *testing.T) {
	s := openTestStore(t)
	err := s.CancelTask(context.Background(), "no-such-id")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}

func TestCancelTask_InvalidState_Running(t *testing.T) {
	s := openTestStore(t)
	task, _ := claimFirst(t, s, "job", 3) // now running

	err := s.CancelTask(context.Background(), task.ID)
	if !errors.Is(err, store.ErrInvalidTransition) {
		t.Errorf("err = %v, want ErrInvalidTransition", err)
	}
}

// ── RetryTask ─────────────────────────────────────────────────────────────────

func TestRetryTask_FromDead(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	task := createTask(t, s, "job")
	forceTaskStatus(t, s, task.ID, store.TaskStatusDead)
	forceRetryCount(t, s, task.ID, 3)

	if err := s.RetryTask(ctx, task.ID); err != nil {
		t.Fatalf("RetryTask: %v", err)
	}

	fresh, _, _, err := s.GetTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if fresh.Status != store.TaskStatusQueued {
		t.Errorf("status = %q, want queued", fresh.Status)
	}
	if fresh.RetryCount != 0 {
		t.Errorf("retry_count = %d, want 0 (reset)", fresh.RetryCount)
	}

	ev := lastEvent(t, s, task.ID)
	if ev.Event != store.TaskEventQueued {
		t.Errorf("last event = %q, want queued", ev.Event)
	}
}

func TestRetryTask_FromCancelled(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	task := createTask(t, s, "job")
	forceTaskStatus(t, s, task.ID, store.TaskStatusCancelled)
	forceRetryCount(t, s, task.ID, 2)

	if err := s.RetryTask(ctx, task.ID); err != nil {
		t.Fatalf("RetryTask: %v", err)
	}

	fresh, _, _, err := s.GetTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if fresh.Status != store.TaskStatusQueued {
		t.Errorf("status = %q, want queued", fresh.Status)
	}
	if fresh.RetryCount != 0 {
		t.Errorf("retry_count = %d, want 0 (reset)", fresh.RetryCount)
	}
}

func TestRetryTask_IsClaimableAfterRetry(t *testing.T) {
	s := openTestStore(t)
	task := createTask(t, s, "job")
	forceTaskStatus(t, s, task.ID, store.TaskStatusDead)

	if err := s.RetryTask(context.Background(), task.ID); err != nil {
		t.Fatalf("RetryTask: %v", err)
	}

	w := createWorker(t, s, []string{"job"})
	got := claim(t, s, w.ID, []string{"job"}, unlimitedCfg())
	if got == nil {
		t.Fatal("expected retried task to be claimable, got nil")
	}
	if got.ID != task.ID {
		t.Errorf("claimed %q, want %q", got.ID, task.ID)
	}
}

func TestRetryTask_NotFound(t *testing.T) {
	s := openTestStore(t)
	err := s.RetryTask(context.Background(), "no-such-id")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}

func TestRetryTask_InvalidState_Queued(t *testing.T) {
	s := openTestStore(t)
	task := createTask(t, s, "job") // already queued

	err := s.RetryTask(context.Background(), task.ID)
	if !errors.Is(err, store.ErrInvalidTransition) {
		t.Errorf("err = %v, want ErrInvalidTransition", err)
	}
}

// ── RestartTask ───────────────────────────────────────────────────────────────

func TestRestartTask_Success(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	orig := createTask(t, s, "crawl")

	newTask, err := s.RestartTask(ctx, orig.ID)
	if err != nil {
		t.Fatalf("RestartTask: %v", err)
	}
	if newTask == nil {
		t.Fatal("RestartTask returned nil task")
	}

	// Original must be superseded.
	origFresh, _, _, err := s.GetTask(ctx, orig.ID)
	if err != nil {
		t.Fatalf("GetTask original: %v", err)
	}
	if origFresh.Status != store.TaskStatusSuperseded {
		t.Errorf("original status = %q, want superseded", origFresh.Status)
	}

	// New task must be queued with parent_id pointing to original.
	if newTask.Status != store.TaskStatusQueued {
		t.Errorf("new task status = %q, want queued", newTask.Status)
	}
	if newTask.ParentID == nil || *newTask.ParentID != orig.ID {
		t.Errorf("new task ParentID = %v, want %q", newTask.ParentID, orig.ID)
	}
	if newTask.ID == orig.ID {
		t.Error("new task ID must differ from original")
	}
}

func TestRestartTask_FieldsCopied(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	orig, err := s.CreateTask(ctx, store.CreateTaskInput{
		Type:       "process",
		Queue:      "batch",
		Payload:    []byte(`{"k":"v"}`),
		Priority:   7,
		MaxRetries: 5,
	})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	newTask, err := s.RestartTask(ctx, orig.ID)
	if err != nil {
		t.Fatalf("RestartTask: %v", err)
	}

	if newTask.Type != orig.Type {
		t.Errorf("Type = %q, want %q", newTask.Type, orig.Type)
	}
	if newTask.Queue != orig.Queue {
		t.Errorf("Queue = %q, want %q", newTask.Queue, orig.Queue)
	}
	if string(newTask.Payload) != string(orig.Payload) {
		t.Errorf("Payload = %q, want %q", newTask.Payload, orig.Payload)
	}
	if newTask.Priority != orig.Priority {
		t.Errorf("Priority = %d, want %d", newTask.Priority, orig.Priority)
	}
	if newTask.MaxRetries != orig.MaxRetries {
		t.Errorf("MaxRetries = %d, want %d", newTask.MaxRetries, orig.MaxRetries)
	}
	if newTask.RetryCount != 0 {
		t.Errorf("RetryCount = %d, want 0", newTask.RetryCount)
	}
}

func TestRestartTask_NewTaskHasQueuedEvent(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	orig := createTask(t, s, "job")

	newTask, err := s.RestartTask(ctx, orig.ID)
	if err != nil {
		t.Fatalf("RestartTask: %v", err)
	}

	_, _, events, err := s.GetTask(ctx, newTask.ID)
	if err != nil {
		t.Fatalf("GetTask new task: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("len(events) = %d, want 1", len(events))
	}
	if events[0].Event != store.TaskEventQueued {
		t.Errorf("event = %q, want queued", events[0].Event)
	}
}

func TestRestartTask_IsClaimable(t *testing.T) {
	s := openTestStore(t)
	orig := createTask(t, s, "job")

	newTask, err := s.RestartTask(context.Background(), orig.ID)
	if err != nil {
		t.Fatalf("RestartTask: %v", err)
	}

	w := createWorker(t, s, []string{"job"})
	got := claim(t, s, w.ID, []string{"job"}, unlimitedCfg())
	if got == nil {
		t.Fatal("expected restarted task to be claimable, got nil")
	}
	if got.ID != newTask.ID {
		t.Errorf("claimed %q, want new task %q", got.ID, newTask.ID)
	}
}

func TestRestartTask_FromSucceeded(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	task, attemptID := claimFirst(t, s, "job", 3)
	if err := s.SucceedAttempt(ctx, attemptID, nil); err != nil {
		t.Fatalf("SucceedAttempt: %v", err)
	}

	newTask, err := s.RestartTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("RestartTask from succeeded: %v", err)
	}
	if newTask == nil {
		t.Fatal("expected non-nil task")
	}
	if newTask.Status != store.TaskStatusQueued {
		t.Errorf("new task status = %q, want queued", newTask.Status)
	}
}

func TestRestartTask_FromHalted(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	id := haltedTask(t, s)

	newTask, err := s.RestartTask(ctx, id)
	if err != nil {
		t.Fatalf("RestartTask from halted: %v", err)
	}
	if newTask.Status != store.TaskStatusQueued {
		t.Errorf("new task status = %q, want queued", newTask.Status)
	}
}

func TestRestartTask_NotFound(t *testing.T) {
	s := openTestStore(t)
	_, err := s.RestartTask(context.Background(), "no-such-id")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}

func TestRestartTask_InvalidState_Running(t *testing.T) {
	s := openTestStore(t)
	task, _ := claimFirst(t, s, "job", 3) // running

	_, err := s.RestartTask(context.Background(), task.ID)
	if !errors.Is(err, store.ErrInvalidTransition) {
		t.Errorf("err = %v, want ErrInvalidTransition", err)
	}
}

func TestRestartTask_InvalidState_Superseded(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	orig := createTask(t, s, "job")

	// First restart supersedes orig.
	if _, err := s.RestartTask(ctx, orig.ID); err != nil {
		t.Fatalf("first RestartTask: %v", err)
	}

	// Second restart on the now-superseded original must fail.
	_, err := s.RestartTask(ctx, orig.ID)
	if !errors.Is(err, store.ErrInvalidTransition) {
		t.Errorf("err = %v, want ErrInvalidTransition", err)
	}
}
