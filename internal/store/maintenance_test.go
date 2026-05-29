package store_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/peifengstudio/erminetq/internal/store"
)

// ── RequeueRetrying ───────────────────────────────────────────────────────────

func TestRequeueRetrying_Basic(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	// Create a task and force it into retrying with run_at in the past.
	task := createTask(t, s, "job")
	past := time.Now().UTC().Add(-time.Second).Format("2006-01-02 15:04:05")
	if _, err := s.DB().Exec(
		`UPDATE tasks SET status = 'retrying', run_at = ? WHERE id = ?`, past, task.ID,
	); err != nil {
		t.Fatalf("setup retrying task: %v", err)
	}

	n, err := s.RequeueRetrying(ctx)
	if err != nil {
		t.Fatalf("RequeueRetrying: %v", err)
	}
	if n != 1 {
		t.Errorf("count = %d, want 1", n)
	}

	fresh, _, _, err := s.GetTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if fresh.Status != store.TaskStatusQueued {
		t.Errorf("status = %q, want queued", fresh.Status)
	}
}

func TestRequeueRetrying_SkipsFutureRunAt(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	task := createTask(t, s, "job")
	future := time.Now().UTC().Add(time.Hour).Format("2006-01-02 15:04:05")
	if _, err := s.DB().Exec(
		`UPDATE tasks SET status = 'retrying', run_at = ? WHERE id = ?`, future, task.ID,
	); err != nil {
		t.Fatalf("setup future retrying task: %v", err)
	}

	n, err := s.RequeueRetrying(ctx)
	if err != nil {
		t.Fatalf("RequeueRetrying: %v", err)
	}
	if n != 0 {
		t.Errorf("count = %d, want 0 (future run_at should not be requeued)", n)
	}

	fresh, _, _, err := s.GetTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if fresh.Status != store.TaskStatusRetrying {
		t.Errorf("status = %q, want retrying (unchanged)", fresh.Status)
	}
}

func TestRequeueRetrying_MultipleTasksBatch(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	past := time.Now().UTC().Add(-time.Second).Format("2006-01-02 15:04:05")

	for i := 0; i < 3; i++ {
		task := createTask(t, s, "job")
		if _, err := s.DB().Exec(
			`UPDATE tasks SET status = 'retrying', run_at = ? WHERE id = ?`, past, task.ID,
		); err != nil {
			t.Fatalf("setup task %d: %v", i, err)
		}
	}

	n, err := s.RequeueRetrying(ctx)
	if err != nil {
		t.Fatalf("RequeueRetrying: %v", err)
	}
	if n != 3 {
		t.Errorf("count = %d, want 3", n)
	}
}

func TestRequeueRetrying_EmptyReturnsZero(t *testing.T) {
	s := openTestStore(t)
	n, err := s.RequeueRetrying(context.Background())
	if err != nil {
		t.Fatalf("RequeueRetrying on empty DB: %v", err)
	}
	if n != 0 {
		t.Errorf("count = %d, want 0", n)
	}
}

// ── FindStaleAttempts ─────────────────────────────────────────────────────────

func TestFindStaleAttempts_FindsStaleByHeartbeat(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	_, attemptID := claimFirst(t, s, "job", 3)

	// Backdate heartbeat_at so the attempt looks stale.
	staleTime := time.Now().UTC().Add(-10 * time.Minute).Format("2006-01-02 15:04:05")
	if _, err := s.DB().Exec(
		`UPDATE attempts SET heartbeat_at = ? WHERE id = ?`, staleTime, attemptID,
	); err != nil {
		t.Fatalf("backdate heartbeat: %v", err)
	}

	cutoff := time.Now().UTC().Add(-5 * time.Minute)
	attempts, err := s.FindStaleAttempts(ctx, cutoff)
	if err != nil {
		t.Fatalf("FindStaleAttempts: %v", err)
	}
	if len(attempts) != 1 {
		t.Fatalf("len = %d, want 1", len(attempts))
	}
	if attempts[0].ID != attemptID {
		t.Errorf("attempt ID = %q, want %q", attempts[0].ID, attemptID)
	}
}

func TestFindStaleAttempts_FindsStaleByStartedAt(t *testing.T) {
	// No heartbeat ever sent; stale is determined by started_at.
	s := openTestStore(t)
	ctx := context.Background()
	_, attemptID := claimFirst(t, s, "job", 3)

	staleTime := time.Now().UTC().Add(-10 * time.Minute).Format("2006-01-02 15:04:05")
	if _, err := s.DB().Exec(
		`UPDATE attempts SET started_at = ?, heartbeat_at = NULL WHERE id = ?`,
		staleTime, attemptID,
	); err != nil {
		t.Fatalf("backdate started_at: %v", err)
	}

	cutoff := time.Now().UTC().Add(-5 * time.Minute)
	attempts, err := s.FindStaleAttempts(ctx, cutoff)
	if err != nil {
		t.Fatalf("FindStaleAttempts: %v", err)
	}
	if len(attempts) != 1 {
		t.Fatalf("len = %d, want 1", len(attempts))
	}
	if attempts[0].ID != attemptID {
		t.Errorf("attempt ID = %q, want %q", attempts[0].ID, attemptID)
	}
}

func TestFindStaleAttempts_SkipsFreshAttempt(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	claimFirst(t, s, "job", 3) // heartbeat_at is nil, started_at is just now

	cutoff := time.Now().UTC().Add(-5 * time.Minute)
	attempts, err := s.FindStaleAttempts(ctx, cutoff)
	if err != nil {
		t.Fatalf("FindStaleAttempts: %v", err)
	}
	if len(attempts) != 0 {
		t.Errorf("len = %d, want 0 (attempt is fresh)", len(attempts))
	}
}

func TestFindStaleAttempts_SkipsFinishedAttempts(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	task, attemptID := claimFirst(t, s, "job", 3)

	// Succeed the attempt so it is no longer running.
	if err := s.SucceedAttempt(ctx, attemptID, nil); err != nil {
		t.Fatalf("SucceedAttempt: %v", err)
	}
	_ = task

	cutoff := time.Now().UTC().Add(time.Hour) // very generous cutoff
	attempts, err := s.FindStaleAttempts(ctx, cutoff)
	if err != nil {
		t.Fatalf("FindStaleAttempts: %v", err)
	}
	if len(attempts) != 0 {
		t.Errorf("len = %d, want 0 (attempt is succeeded, not running)", len(attempts))
	}
}

// ── TimeoutAttempt ────────────────────────────────────────────────────────────

func TestTimeoutAttempt_GoesRetrying(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	task, attemptID := claimFirst(t, s, "job", 3) // max_retries=3

	if err := s.TimeoutAttempt(ctx, attemptID); err != nil {
		t.Fatalf("TimeoutAttempt: %v", err)
	}

	fresh, _, _, err := s.GetTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if fresh.Status != store.TaskStatusRetrying {
		t.Errorf("task status = %q, want retrying", fresh.Status)
	}

	ev := lastEvent(t, s, task.ID)
	if ev.Event != store.TaskEventHeartbeatTimeout {
		t.Errorf("last event = %q, want heartbeat_timeout", ev.Event)
	}
}

func TestTimeoutAttempt_GoesDead(t *testing.T) {
	// max_retries=0 → defaults to 3; force retry_count=3 so there are no retries left.
	s := openTestStore(t)
	ctx := context.Background()
	task, attemptID := claimFirst(t, s, "job", 3)
	forceRetryCount(t, s, task.ID, 3) // retry_count == max_retries → goes dead

	if err := s.TimeoutAttempt(ctx, attemptID); err != nil {
		t.Fatalf("TimeoutAttempt: %v", err)
	}

	fresh, _, _, err := s.GetTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if fresh.Status != store.TaskStatusDead {
		t.Errorf("task status = %q, want dead", fresh.Status)
	}

	ev := lastEvent(t, s, task.ID)
	if ev.Event != store.TaskEventHeartbeatTimeout {
		t.Errorf("last event = %q, want heartbeat_timeout", ev.Event)
	}
}

func TestTimeoutAttempt_AttemptMarkedFailed(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	_, attemptID := claimFirst(t, s, "job", 3)

	if err := s.TimeoutAttempt(ctx, attemptID); err != nil {
		t.Fatalf("TimeoutAttempt: %v", err)
	}

	row := s.DB().QueryRow(
		`SELECT status, error FROM attempts WHERE id = ?`, attemptID,
	)
	var status, errMsg string
	if err := row.Scan(&status, &errMsg); err != nil {
		t.Fatalf("scan attempt: %v", err)
	}
	if status != "failed" {
		t.Errorf("attempt status = %q, want failed", status)
	}
	if errMsg != "heartbeat timeout" {
		t.Errorf("attempt error = %q, want %q", errMsg, "heartbeat timeout")
	}
}

func TestTimeoutAttempt_NotRunning(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	_, attemptID := claimFirst(t, s, "job", 3)
	if err := s.SucceedAttempt(ctx, attemptID, nil); err != nil {
		t.Fatalf("SucceedAttempt: %v", err)
	}

	err := s.TimeoutAttempt(ctx, attemptID)
	if !errors.Is(err, store.ErrAttemptNotRunning) {
		t.Errorf("err = %v, want ErrAttemptNotRunning", err)
	}
}

// ── DueSchedules ─────────────────────────────────────────────────────────────

func TestDueSchedules_ReturnsDue(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	past := time.Now().UTC().Add(-time.Second)
	sc, err := s.CreateSchedule(ctx, store.CreateScheduleInput{
		TaskType:     "sync",
		Enabled:      true,
		IntervalSecs: ptr(60),
		NextRunAt:    &past,
	})
	if err != nil {
		t.Fatalf("CreateSchedule: %v", err)
	}

	due, err := s.DueSchedules(ctx)
	if err != nil {
		t.Fatalf("DueSchedules: %v", err)
	}
	if len(due) != 1 {
		t.Fatalf("len = %d, want 1", len(due))
	}
	if due[0].ID != sc.ID {
		t.Errorf("schedule ID = %q, want %q", due[0].ID, sc.ID)
	}
}

func TestDueSchedules_SkipsFutureSchedule(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	future := time.Now().UTC().Add(time.Hour)
	if _, err := s.CreateSchedule(ctx, store.CreateScheduleInput{
		TaskType:     "sync",
		Enabled:      true,
		IntervalSecs: ptr(3600),
		NextRunAt:    &future,
	}); err != nil {
		t.Fatalf("CreateSchedule: %v", err)
	}

	due, err := s.DueSchedules(ctx)
	if err != nil {
		t.Fatalf("DueSchedules: %v", err)
	}
	if len(due) != 0 {
		t.Errorf("len = %d, want 0 (next_run_at is in the future)", len(due))
	}
}

func TestDueSchedules_SkipsDisabled(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	past := time.Now().UTC().Add(-time.Second)
	if _, err := s.CreateSchedule(ctx, store.CreateScheduleInput{
		TaskType:     "sync",
		Enabled:      false,
		IntervalSecs: ptr(60),
		NextRunAt:    &past,
	}); err != nil {
		t.Fatalf("CreateSchedule: %v", err)
	}

	due, err := s.DueSchedules(ctx)
	if err != nil {
		t.Fatalf("DueSchedules: %v", err)
	}
	if len(due) != 0 {
		t.Errorf("len = %d, want 0 (schedule is disabled)", len(due))
	}
}

func TestDueSchedules_Empty(t *testing.T) {
	s := openTestStore(t)
	due, err := s.DueSchedules(context.Background())
	if err != nil {
		t.Fatalf("DueSchedules on empty DB: %v", err)
	}
	if len(due) != 0 {
		t.Errorf("len = %d, want 0", len(due))
	}
}

// ── RecordScheduleRun ─────────────────────────────────────────────────────────

func TestRecordScheduleRun_UpdatesTimestamps(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	past := time.Now().UTC().Add(-time.Second)
	sc, err := s.CreateSchedule(ctx, store.CreateScheduleInput{
		TaskType:     "sync",
		Enabled:      true,
		IntervalSecs: ptr(60),
		NextRunAt:    &past,
	})
	if err != nil {
		t.Fatalf("CreateSchedule: %v", err)
	}

	nextRun := time.Now().UTC().Add(time.Minute).Truncate(time.Second)
	if err := s.RecordScheduleRun(ctx, sc.ID, nextRun); err != nil {
		t.Fatalf("RecordScheduleRun: %v", err)
	}

	fresh, err := s.GetSchedule(ctx, sc.ID)
	if err != nil {
		t.Fatalf("GetSchedule: %v", err)
	}
	if fresh.LastRunAt == nil {
		t.Fatal("last_run_at should not be nil after RecordScheduleRun")
	}
	if fresh.NextRunAt == nil {
		t.Fatal("next_run_at should not be nil")
	}
	if !fresh.NextRunAt.Equal(nextRun) {
		t.Errorf("next_run_at = %v, want %v", fresh.NextRunAt, nextRun)
	}
}

func TestRecordScheduleRun_NotFound(t *testing.T) {
	s := openTestStore(t)
	err := s.RecordScheduleRun(context.Background(), "no-such-id", time.Now())
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}

func TestRecordScheduleRun_MakesScheduleNotDue(t *testing.T) {
	// After recording a run, the schedule should no longer appear in DueSchedules.
	s := openTestStore(t)
	ctx := context.Background()

	past := time.Now().UTC().Add(-time.Second)
	sc, err := s.CreateSchedule(ctx, store.CreateScheduleInput{
		TaskType:     "sync",
		Enabled:      true,
		IntervalSecs: ptr(60),
		NextRunAt:    &past,
	})
	if err != nil {
		t.Fatalf("CreateSchedule: %v", err)
	}

	nextRun := time.Now().UTC().Add(time.Hour)
	if err := s.RecordScheduleRun(ctx, sc.ID, nextRun); err != nil {
		t.Fatalf("RecordScheduleRun: %v", err)
	}

	due, err := s.DueSchedules(ctx)
	if err != nil {
		t.Fatalf("DueSchedules: %v", err)
	}
	if len(due) != 0 {
		t.Errorf("schedule should no longer be due after RecordScheduleRun, got %d", len(due))
	}
}
