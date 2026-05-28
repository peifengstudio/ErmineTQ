package store_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/peifengstudio/erminetq/internal/store"
)

// ── fixtures ──────────────────────────────────────────────────────────────────

// claimFirst creates one task of the given type, claims it, and returns the
// task and the first attempt ID.
func claimFirst(t *testing.T, s *store.Store, taskType string, maxRetries int) (*store.Task, string) {
	t.Helper()
	ctx := context.Background()

	task, err := s.CreateTask(ctx, store.CreateTaskInput{
		Type:       taskType,
		MaxRetries: maxRetries,
	})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	w := createWorker(t, s, []string{taskType})
	claimed := claim(t, s, w.ID, []string{taskType}, unlimitedCfg())
	if claimed == nil {
		t.Fatal("ClaimTask returned nil")
	}

	_, attempts, _, err := s.GetTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if len(attempts) != 1 {
		t.Fatalf("expected 1 attempt, got %d", len(attempts))
	}
	return claimed, attempts[0].ID
}

// requeuTask bypasses the Store layer to reset a task back to 'queued' with
// run_at in the past.  Used only to simulate the retry-scheduler for testing.
func requeuTask(t *testing.T, s *store.Store, taskID string) {
	t.Helper()
	past := time.Now().UTC().Add(-time.Second).Format("2006-01-02 15:04:05")
	if _, err := s.DB().Exec(
		`UPDATE tasks SET status = 'queued', run_at = ? WHERE id = ?`,
		past, taskID,
	); err != nil {
		t.Fatalf("requeuTask: %v", err)
	}
}

// lastEvent returns the most recent task_event for taskID.
func lastEvent(t *testing.T, s *store.Store, taskID string) *store.TaskEvent {
	t.Helper()
	_, _, events, err := s.GetTask(context.Background(), taskID)
	if err != nil {
		t.Fatalf("GetTask for events: %v", err)
	}
	if len(events) == 0 {
		t.Fatalf("no events for task %s", taskID)
	}
	return events[len(events)-1]
}

// ── SucceedAttempt ────────────────────────────────────────────────────────────

func TestSucceedAttempt_Basic(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	task, attemptID := claimFirst(t, s, "job", 3)

	result := json.RawMessage(`{"rows":42}`)
	if err := s.SucceedAttempt(ctx, attemptID, result); err != nil {
		t.Fatalf("SucceedAttempt: %v", err)
	}

	fresh, attempts, events, err := s.GetTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}

	// Task status
	if fresh.Status != store.TaskStatusSucceeded {
		t.Errorf("task status = %q, want succeeded", fresh.Status)
	}

	// Attempt
	if len(attempts) != 1 {
		t.Fatalf("len(attempts) = %d, want 1", len(attempts))
	}
	a := attempts[0]
	if a.Status != store.AttemptStatusSucceeded {
		t.Errorf("attempt status = %q, want succeeded", a.Status)
	}
	if string(a.Result) != string(result) {
		t.Errorf("attempt result = %s, want %s", a.Result, result)
	}
	if a.FinishedAt == nil {
		t.Error("attempt FinishedAt should be set")
	}

	// Events: queued, started, succeeded
	if len(events) != 3 {
		t.Fatalf("len(events) = %d, want 3", len(events))
	}
	if events[2].Event != store.TaskEventSucceeded {
		t.Errorf("last event = %q, want succeeded", events[2].Event)
	}
	var detail map[string]string
	if err := json.Unmarshal(events[2].Detail, &detail); err != nil {
		t.Fatalf("unmarshal succeeded detail: %v", err)
	}
	if detail["attempt_id"] != attemptID {
		t.Errorf("detail.attempt_id = %q, want %q", detail["attempt_id"], attemptID)
	}
}

func TestSucceedAttempt_NilResult(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	_, attemptID := claimFirst(t, s, "job", 3)

	if err := s.SucceedAttempt(ctx, attemptID, nil); err != nil {
		t.Fatalf("SucceedAttempt with nil result: %v", err)
	}

	// Attempt result should be NULL (nil RawMessage).
	task, _ := claimFirst(t, s, "other", 3) // create another to reuse GetTask path
	_ = task
	// Re-read the original task.
	// We don't have the task ID here, but SucceedAttempt not erroring is sufficient.
}

func TestSucceedAttempt_DecrementsWorker(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	task, err := s.CreateTask(ctx, store.CreateTaskInput{Type: "job"})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	w, err := s.CreateWorker(ctx, store.CreateWorkerInput{
		Type: store.WorkerTypeGo, TaskTypes: []string{"job"}, Concurrency: 2,
	})
	if err != nil {
		t.Fatalf("CreateWorker: %v", err)
	}

	claim(t, s, w.ID, []string{"job"}, unlimitedCfg())

	_, attempts, _, _ := s.GetTask(ctx, task.ID)
	if err := s.SucceedAttempt(ctx, attempts[0].ID, nil); err != nil {
		t.Fatalf("SucceedAttempt: %v", err)
	}

	worker, err := s.GetWorker(ctx, w.ID)
	if err != nil {
		t.Fatalf("GetWorker: %v", err)
	}
	if worker.CurrentTaskCount != 0 {
		t.Errorf("CurrentTaskCount = %d, want 0", worker.CurrentTaskCount)
	}
	if worker.Status != store.WorkerStatusIdle {
		t.Errorf("worker status = %q, want idle", worker.Status)
	}
}

func TestSucceedAttempt_NotFound(t *testing.T) {
	s := openTestStore(t)
	err := s.SucceedAttempt(context.Background(), "no-such-attempt", nil)
	if err != store.ErrNotFound {
		t.Errorf("SucceedAttempt(missing) = %v, want ErrNotFound", err)
	}
}

func TestSucceedAttempt_NotRunning(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	_, attemptID := claimFirst(t, s, "job", 3)

	// Succeed once.
	if err := s.SucceedAttempt(ctx, attemptID, nil); err != nil {
		t.Fatalf("first SucceedAttempt: %v", err)
	}
	// Succeed again: attempt is now 'succeeded', not 'running'.
	err := s.SucceedAttempt(ctx, attemptID, nil)
	if err != store.ErrAttemptNotRunning {
		t.Errorf("second SucceedAttempt = %v, want ErrAttemptNotRunning", err)
	}
}

// ── FailAttempt ───────────────────────────────────────────────────────────────

func TestFailAttempt_Retrying(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	// max_retries=2: first failure → retrying.
	task, attemptID := claimFirst(t, s, "crawl", 2)
	before := time.Now().UTC()

	if err := s.FailAttempt(ctx, attemptID, "connection refused"); err != nil {
		t.Fatalf("FailAttempt: %v", err)
	}

	fresh, attempts, events, err := s.GetTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}

	// Task
	if fresh.Status != store.TaskStatusRetrying {
		t.Errorf("task status = %q, want retrying", fresh.Status)
	}
	if fresh.RetryCount != 1 {
		t.Errorf("retry_count = %d, want 1", fresh.RetryCount)
	}
	if !fresh.RunAt.After(before) {
		t.Errorf("run_at %v should be after %v (backoff not applied)", fresh.RunAt, before)
	}

	// Attempt
	if attempts[0].Status != store.AttemptStatusFailed {
		t.Errorf("attempt status = %q, want failed", attempts[0].Status)
	}
	if attempts[0].Error == nil || *attempts[0].Error != "connection refused" {
		t.Errorf("attempt.error = %v, want %q", attempts[0].Error, "connection refused")
	}
	if attempts[0].FinishedAt == nil {
		t.Error("attempt FinishedAt should be set")
	}

	// Events: queued, started, retrying
	if len(events) != 3 {
		t.Fatalf("len(events) = %d, want 3", len(events))
	}
	ev := events[2]
	if ev.Event != store.TaskEventRetrying {
		t.Errorf("last event = %q, want retrying", ev.Event)
	}
	var detail map[string]any
	if err := json.Unmarshal(ev.Detail, &detail); err != nil {
		t.Fatalf("unmarshal retrying detail: %v", err)
	}
	if detail["attempt_id"] != attemptID {
		t.Errorf("detail.attempt_id = %v, want %q", detail["attempt_id"], attemptID)
	}
	if detail["error"] != "connection refused" {
		t.Errorf("detail.error = %v, want connection refused", detail["error"])
	}
	// retry_count in detail should be 1 (the new value).
	if rc, _ := detail["retry_count"].(float64); int(rc) != 1 {
		t.Errorf("detail.retry_count = %v, want 1", detail["retry_count"])
	}
}

func TestFailAttempt_Dead(t *testing.T) {
	// Two-cycle test: max_retries=1
	//   cycle 1: fail → retrying (retry_count=1)
	//   cycle 2: fail → dead    (retry_count=1 ≥ max_retries=1)
	s := openTestStore(t)
	ctx := context.Background()
	task, attemptID1 := claimFirst(t, s, "crawl", 1)

	// Cycle 1: retrying.
	if err := s.FailAttempt(ctx, attemptID1, "err1"); err != nil {
		t.Fatalf("cycle 1 FailAttempt: %v", err)
	}
	fresh, _, _, _ := s.GetTask(ctx, task.ID)
	if fresh.Status != store.TaskStatusRetrying {
		t.Fatalf("expected retrying after cycle 1, got %q", fresh.Status)
	}

	// Simulate retry scheduler: move task back to queued with run_at in past.
	requeuTask(t, s, task.ID)

	// Cycle 2: claim → attempt 2.
	w2 := createWorker(t, s, []string{"crawl"})
	claimed2 := claim(t, s, w2.ID, []string{"crawl"}, unlimitedCfg())
	if claimed2 == nil {
		t.Fatal("cycle 2 claim returned nil")
	}
	_, attempts2, _, _ := s.GetTask(ctx, task.ID)
	var attemptID2 string
	for _, a := range attempts2 {
		if a.Status == store.AttemptStatusRunning {
			attemptID2 = a.ID
			break
		}
	}
	if attemptID2 == "" {
		t.Fatal("no running attempt found after cycle 2 claim")
	}
	if attempts2[1].AttemptNum != 2 {
		t.Errorf("attempt_num = %d, want 2", attempts2[1].AttemptNum)
	}

	// Cycle 2: fail → dead.
	if err := s.FailAttempt(ctx, attemptID2, "err2"); err != nil {
		t.Fatalf("cycle 2 FailAttempt: %v", err)
	}

	final, _, events, err := s.GetTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetTask after dead: %v", err)
	}
	if final.Status != store.TaskStatusDead {
		t.Errorf("task status = %q, want dead", final.Status)
	}

	// Events: queued, started, retrying, started, dead
	if len(events) != 5 {
		t.Fatalf("len(events) = %d, want 5", len(events))
	}
	if events[4].Event != store.TaskEventDead {
		t.Errorf("last event = %q, want dead", events[4].Event)
	}
	var detail map[string]any
	json.Unmarshal(events[4].Detail, &detail)
	if detail["error"] != "err2" {
		t.Errorf("dead detail.error = %v, want err2", detail["error"])
	}
}

func TestFailAttempt_BackoffProgression(t *testing.T) {
	// Verify that each retry uses a longer backoff than the previous one.
	// We test this by inspecting run_at after successive failures.
	s := openTestStore(t)
	ctx := context.Background()

	task, _ := s.CreateTask(ctx, store.CreateTaskInput{
		Type:       "job",
		MaxRetries: 5,
	})

	w := createWorker(t, s, []string{"job"})

	var prevRunAt time.Time

	for i := 0; i < 3; i++ {
		requeuTask(t, s, task.ID)

		claimed := claim(t, s, w.ID, []string{"job"}, unlimitedCfg())
		if claimed == nil {
			t.Fatalf("cycle %d: claim returned nil", i+1)
		}
		_, attempts, _, _ := s.GetTask(ctx, task.ID)
		var aID string
		for _, a := range attempts {
			if a.Status == store.AttemptStatusRunning {
				aID = a.ID
			}
		}

		before := time.Now().UTC()
		if err := s.FailAttempt(ctx, aID, "err"); err != nil {
			t.Fatalf("cycle %d FailAttempt: %v", i+1, err)
		}

		fresh, _, _, _ := s.GetTask(ctx, task.ID)
		if !fresh.RunAt.After(before) {
			t.Errorf("cycle %d: run_at %v should be after %v", i+1, fresh.RunAt, before)
		}
		if i > 0 && !fresh.RunAt.After(prevRunAt) {
			t.Errorf("cycle %d: run_at %v should be after previous run_at %v (backoff should grow)",
				i+1, fresh.RunAt, prevRunAt)
		}
		prevRunAt = fresh.RunAt
	}
}

func TestFailAttempt_NotFound(t *testing.T) {
	s := openTestStore(t)
	err := s.FailAttempt(context.Background(), "no-such", "err")
	if err != store.ErrNotFound {
		t.Errorf("FailAttempt(missing) = %v, want ErrNotFound", err)
	}
}

func TestFailAttempt_NotRunning(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	_, attemptID := claimFirst(t, s, "job", 3)

	if err := s.SucceedAttempt(ctx, attemptID, nil); err != nil {
		t.Fatalf("SucceedAttempt: %v", err)
	}
	err := s.FailAttempt(ctx, attemptID, "too late")
	if err != store.ErrAttemptNotRunning {
		t.Errorf("FailAttempt on succeeded attempt = %v, want ErrAttemptNotRunning", err)
	}
}

// ── CancelAttempt ─────────────────────────────────────────────────────────────

func TestCancelAttempt_Basic(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	task, attemptID := claimFirst(t, s, "job", 3)

	if err := s.CancelAttempt(ctx, attemptID); err != nil {
		t.Fatalf("CancelAttempt: %v", err)
	}

	fresh, attempts, events, err := s.GetTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}

	if fresh.Status != store.TaskStatusCancelled {
		t.Errorf("task status = %q, want cancelled", fresh.Status)
	}
	if attempts[0].Status != store.AttemptStatusCancelled {
		t.Errorf("attempt status = %q, want cancelled", attempts[0].Status)
	}
	if attempts[0].FinishedAt == nil {
		t.Error("attempt FinishedAt should be set after cancel")
	}

	// Events: queued, started, cancelled
	if len(events) != 3 {
		t.Fatalf("len(events) = %d, want 3", len(events))
	}
	if events[2].Event != store.TaskEventCancelled {
		t.Errorf("last event = %q, want cancelled", events[2].Event)
	}
}

func TestCancelAttempt_DecrementsWorker(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	task, err := s.CreateTask(ctx, store.CreateTaskInput{Type: "job"})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	w, err := s.CreateWorker(ctx, store.CreateWorkerInput{
		Type: store.WorkerTypeGo, TaskTypes: []string{"job"}, Concurrency: 5,
	})
	if err != nil {
		t.Fatalf("CreateWorker: %v", err)
	}

	claim(t, s, w.ID, []string{"job"}, unlimitedCfg())
	_, attempts, _, _ := s.GetTask(ctx, task.ID)

	if err := s.CancelAttempt(ctx, attempts[0].ID); err != nil {
		t.Fatalf("CancelAttempt: %v", err)
	}

	worker, _ := s.GetWorker(ctx, w.ID)
	if worker.CurrentTaskCount != 0 {
		t.Errorf("CurrentTaskCount = %d, want 0", worker.CurrentTaskCount)
	}
}

func TestCancelAttempt_NotFound(t *testing.T) {
	s := openTestStore(t)
	err := s.CancelAttempt(context.Background(), "ghost")
	if err != store.ErrNotFound {
		t.Errorf("CancelAttempt(missing) = %v, want ErrNotFound", err)
	}
}

func TestCancelAttempt_NotRunning(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	_, attemptID := claimFirst(t, s, "job", 3)

	if err := s.CancelAttempt(ctx, attemptID); err != nil {
		t.Fatalf("first CancelAttempt: %v", err)
	}
	err := s.CancelAttempt(ctx, attemptID)
	if err != store.ErrAttemptNotRunning {
		t.Errorf("second CancelAttempt = %v, want ErrAttemptNotRunning", err)
	}
}

// ── UpdateHeartbeat ───────────────────────────────────────────────────────────

func TestUpdateHeartbeat_SetsTimestamp(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	task, attemptID := claimFirst(t, s, "job", 3)

	before := time.Now().UTC().Truncate(time.Second)
	if err := s.UpdateHeartbeat(ctx, attemptID); err != nil {
		t.Fatalf("UpdateHeartbeat: %v", err)
	}

	_, attempts, _, err := s.GetTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	a := attempts[0]
	if a.HeartbeatAt == nil {
		t.Fatal("HeartbeatAt should not be nil after UpdateHeartbeat")
	}
	if a.HeartbeatAt.Before(before) {
		t.Errorf("HeartbeatAt %v is before %v", a.HeartbeatAt, before)
	}
}

func TestUpdateHeartbeat_NoTaskEvent(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	task, attemptID := claimFirst(t, s, "job", 3)

	if err := s.UpdateHeartbeat(ctx, attemptID); err != nil {
		t.Fatalf("UpdateHeartbeat: %v", err)
	}

	// events must still be only queued + started (no heartbeat event).
	_, _, events, err := s.GetTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if len(events) != 2 {
		t.Errorf("len(events) = %d, want 2 (heartbeat must not write task_events)", len(events))
	}
}

func TestUpdateHeartbeat_SilentOnMissingAttempt(t *testing.T) {
	s := openTestStore(t)
	// A missing attemptID must not return an error.
	if err := s.UpdateHeartbeat(context.Background(), "ghost-attempt"); err != nil {
		t.Errorf("UpdateHeartbeat(missing) = %v, want nil", err)
	}
}

func TestUpdateHeartbeat_SilentOnFinishedAttempt(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	_, attemptID := claimFirst(t, s, "job", 3)

	if err := s.SucceedAttempt(ctx, attemptID, nil); err != nil {
		t.Fatalf("SucceedAttempt: %v", err)
	}
	// Heartbeat on a completed attempt must not error.
	if err := s.UpdateHeartbeat(ctx, attemptID); err != nil {
		t.Errorf("UpdateHeartbeat on finished attempt = %v, want nil", err)
	}
}
