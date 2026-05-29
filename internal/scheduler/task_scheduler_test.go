package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/peifengstudio/erminetq/internal/store"
)

// ptrOf is a generic helper that returns a pointer to a copy of v.
func ptrOf[T any](v T) *T { return &v }

// ── computeNextRun ────────────────────────────────────────────────────────────

func TestComputeNextRun_Cron_Basic(t *testing.T) {
	// "0 10 * * *" = every day at 10:00 UTC
	after := time.Date(2025, 1, 15, 9, 0, 0, 0, time.UTC)
	sc := &store.Schedule{ID: "s1", CronExpr: ptrOf("0 10 * * *")}

	next, err := computeNextRun(sc, after)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC)
	if !next.Equal(want) {
		t.Errorf("next = %v, want %v", next, want)
	}
}

func TestComputeNextRun_Cron_WrapsToNextDay(t *testing.T) {
	// after is past 10:00, so next trigger is tomorrow.
	after := time.Date(2025, 1, 15, 11, 0, 0, 0, time.UTC)
	sc := &store.Schedule{ID: "s1", CronExpr: ptrOf("0 10 * * *")}

	next, err := computeNextRun(sc, after)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := time.Date(2025, 1, 16, 10, 0, 0, 0, time.UTC)
	if !next.Equal(want) {
		t.Errorf("next = %v, want %v", next, want)
	}
}

func TestComputeNextRun_Cron_InvalidExpression(t *testing.T) {
	sc := &store.Schedule{ID: "s1", CronExpr: ptrOf("not a cron")}
	_, err := computeNextRun(sc, time.Now())
	if err == nil {
		t.Error("expected error for invalid cron expression, got nil")
	}
}

func TestComputeNextRun_Interval_WithLastRunAt(t *testing.T) {
	lastRun := time.Date(2025, 1, 15, 9, 0, 0, 0, time.UTC)
	sc := &store.Schedule{
		ID:           "s1",
		IntervalSecs: ptrOf(3600), // 1 hour
		LastRunAt:    &lastRun,
	}

	next, err := computeNextRun(sc, time.Now()) // `after` is ignored when LastRunAt is set
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := lastRun.Add(time.Hour)
	if !next.Equal(want) {
		t.Errorf("next = %v, want %v", next, want)
	}
}

func TestComputeNextRun_Interval_NilLastRunAt_UsesAfter(t *testing.T) {
	after := time.Date(2025, 1, 15, 9, 0, 0, 0, time.UTC)
	sc := &store.Schedule{
		ID:           "s1",
		IntervalSecs: ptrOf(3600),
		LastRunAt:    nil, // first run
	}

	next, err := computeNextRun(sc, after)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := after.Add(time.Hour)
	if !next.Equal(want) {
		t.Errorf("next = %v, want %v", next, want)
	}
}

func TestComputeNextRun_NeitherCronNorInterval(t *testing.T) {
	sc := &store.Schedule{ID: "s-bad"}
	_, err := computeNextRun(sc, time.Now())
	if err == nil {
		t.Error("expected error for schedule with neither cron nor interval, got nil")
	}
}

// ── taskScheduler.initSchedules ───────────────────────────────────────────────

func TestTaskScheduler_InitSchedules_SetsNilNextRunAt(t *testing.T) {
	updated := make(chan store.UpdateScheduleInput, 1)
	ms := &mockStore{
		listSchedulesFn: func(_ context.Context) ([]*store.Schedule, error) {
			return []*store.Schedule{{
				ID:           "sched-1",
				Enabled:      true,
				IntervalSecs: ptrOf(60),
				NextRunAt:    nil, // needs initialisation
			}}, nil
		},
		updateScheduleFn: func(_ context.Context, _ string, in store.UpdateScheduleInput) (*store.Schedule, error) {
			updated <- in
			return &store.Schedule{}, nil
		},
	}

	ts := newTaskScheduler(ms, time.Hour, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ts.initSchedules(ctx)

	select {
	case in := <-updated:
		if in.NextRunAt == nil {
			t.Error("UpdateSchedule called with nil NextRunAt")
		}
	default:
		t.Error("UpdateSchedule was not called for nil-next_run_at schedule")
	}
}

func TestTaskScheduler_InitSchedules_SkipsAlreadySet(t *testing.T) {
	nextRun := time.Now().Add(time.Hour)
	var updateCalled bool
	ms := &mockStore{
		listSchedulesFn: func(_ context.Context) ([]*store.Schedule, error) {
			return []*store.Schedule{{
				ID:           "s1",
				Enabled:      true,
				IntervalSecs: ptrOf(60),
				NextRunAt:    &nextRun, // already initialised
			}}, nil
		},
		updateScheduleFn: func(_ context.Context, _ string, _ store.UpdateScheduleInput) (*store.Schedule, error) {
			updateCalled = true
			return nil, nil
		},
	}

	ts := newTaskScheduler(ms, time.Hour, nil)
	ts.initSchedules(context.Background())

	if updateCalled {
		t.Error("UpdateSchedule must not be called when NextRunAt is already set")
	}
}

func TestTaskScheduler_InitSchedules_SkipsDisabled(t *testing.T) {
	var updateCalled bool
	ms := &mockStore{
		listSchedulesFn: func(_ context.Context) ([]*store.Schedule, error) {
			return []*store.Schedule{{
				ID:           "s1",
				Enabled:      false, // disabled
				IntervalSecs: ptrOf(60),
				NextRunAt:    nil,
			}}, nil
		},
		updateScheduleFn: func(_ context.Context, _ string, _ store.UpdateScheduleInput) (*store.Schedule, error) {
			updateCalled = true
			return nil, nil
		},
	}

	ts := newTaskScheduler(ms, time.Hour, nil)
	ts.initSchedules(context.Background())

	if updateCalled {
		t.Error("UpdateSchedule must not be called for disabled schedules")
	}
}

// ── taskScheduler.poll ────────────────────────────────────────────────────────

func TestTaskScheduler_Poll_FiresDueSchedule(t *testing.T) {
	payload := json.RawMessage(`{"key":"val"}`)
	dueSchedule := &store.Schedule{
		ID:           "sched-1",
		TaskType:     "sync",
		Queue:        "default",
		Payload:      payload,
		IntervalSecs: ptrOf(3600),
	}

	createdInput := make(chan store.CreateTaskInput, 1)
	recordedNext := make(chan time.Time, 1)

	ms := &mockStore{
		dueSchedulesFn: func(_ context.Context) ([]*store.Schedule, error) {
			return []*store.Schedule{dueSchedule}, nil
		},
		createTaskFn: func(_ context.Context, in store.CreateTaskInput) (*store.Task, error) {
			createdInput <- in
			return &store.Task{ID: "t-1"}, nil
		},
		recordScheduleRunFn: func(_ context.Context, _ string, nextRunAt time.Time) error {
			recordedNext <- nextRunAt
			return nil
		},
	}

	ts := newTaskScheduler(ms, time.Hour, nil)
	ts.poll(context.Background())

	// CreateTask must be called with the schedule's fields.
	select {
	case in := <-createdInput:
		if in.Type != "sync" {
			t.Errorf("CreateTask type = %q, want %q", in.Type, "sync")
		}
		if in.Queue != "default" {
			t.Errorf("CreateTask queue = %q, want %q", in.Queue, "default")
		}
		if string(in.Payload) != string(payload) {
			t.Errorf("CreateTask payload = %s, want %s", in.Payload, payload)
		}
		if in.ScheduleID == nil || *in.ScheduleID != "sched-1" {
			t.Errorf("CreateTask schedule_id = %v, want %q", in.ScheduleID, "sched-1")
		}
	default:
		t.Fatal("CreateTask was not called")
	}

	// RecordScheduleRun must be called with a future time.
	select {
	case next := <-recordedNext:
		if !next.After(time.Now().Add(-time.Second)) {
			t.Errorf("next_run_at %v is in the past", next)
		}
	default:
		t.Fatal("RecordScheduleRun was not called")
	}
}

func TestTaskScheduler_Poll_NoDueSchedules(t *testing.T) {
	var createCalled bool
	ms := &mockStore{
		dueSchedulesFn: func(_ context.Context) ([]*store.Schedule, error) {
			return nil, nil // nothing due
		},
		createTaskFn: func(_ context.Context, _ store.CreateTaskInput) (*store.Task, error) {
			createCalled = true
			return nil, nil
		},
	}

	ts := newTaskScheduler(ms, time.Hour, nil)
	ts.poll(context.Background())

	if createCalled {
		t.Error("CreateTask must not be called when no schedules are due")
	}
}

func TestTaskScheduler_Poll_CreateTaskError_SkipsRecordRun(t *testing.T) {
	var recordCalled bool
	boom := errors.New("insert failed")
	errCh := make(chan error, 1)

	ms := &mockStore{
		dueSchedulesFn: func(_ context.Context) ([]*store.Schedule, error) {
			return []*store.Schedule{{
				ID: "s1", TaskType: "job", IntervalSecs: ptrOf(60),
			}}, nil
		},
		createTaskFn: func(_ context.Context, _ store.CreateTaskInput) (*store.Task, error) {
			return nil, boom
		},
		recordScheduleRunFn: func(_ context.Context, _ string, _ time.Time) error {
			recordCalled = true
			return nil
		},
	}

	ts := newTaskScheduler(ms, time.Hour, func(err error) {
		errCh <- err
	})
	ts.poll(context.Background())

	if recordCalled {
		t.Error("RecordScheduleRun must not be called after CreateTask failure")
	}
	select {
	case err := <-errCh:
		if !errors.Is(err, boom) {
			t.Errorf("onError got %v, want %v", err, boom)
		}
	default:
		t.Error("onError was not called after CreateTask failure")
	}
}

// ── taskScheduler lifecycle ────────────────────────────────────────────────────

func TestTaskScheduler_TickFiresPolling(t *testing.T) {
	polled := make(chan struct{}, 1)
	ms := &mockStore{
		listSchedulesFn: func(_ context.Context) ([]*store.Schedule, error) {
			return nil, nil // no init needed
		},
		dueSchedulesFn: func(_ context.Context) ([]*store.Schedule, error) {
			select {
			case polled <- struct{}{}:
			default:
			}
			return nil, nil
		},
	}

	ts := newTaskScheduler(ms, 5*time.Millisecond, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	ts.start(ctx)

	select {
	case <-polled:
		// poll loop fired
	case <-ctx.Done():
		t.Fatal("DueSchedules was not called within timeout")
	}

	cancel()
	ts.wait()
}

func TestTaskScheduler_StopsOnCtxCancel(t *testing.T) {
	ms := &mockStore{
		listSchedulesFn: func(_ context.Context) ([]*store.Schedule, error) { return nil, nil },
	}

	ts := newTaskScheduler(ms, 10*time.Millisecond, nil)

	ctx, cancel := context.WithCancel(context.Background())
	ts.start(ctx)
	cancel()

	done := make(chan struct{})
	go func() { ts.wait(); close(done) }()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("taskScheduler did not stop after ctx cancellation")
	}
}

// ── Scheduler integration — all three components ─────────────────────────────

func TestScheduler_AllThreeComponentsRun(t *testing.T) {
	requeued := make(chan struct{}, 1)
	scanned := make(chan struct{}, 1)
	polled := make(chan struct{}, 1)

	ms := &mockStore{
		requeueRetryingFn: func(_ context.Context) (int, error) {
			select {
			case requeued <- struct{}{}:
			default:
			}
			return 0, nil
		},
		findStaleAttemptsFn: func(_ context.Context, _ time.Time) ([]*store.Attempt, error) {
			select {
			case scanned <- struct{}{}:
			default:
			}
			return nil, nil
		},
		listSchedulesFn: func(_ context.Context) ([]*store.Schedule, error) {
			return nil, nil
		},
		dueSchedulesFn: func(_ context.Context) ([]*store.Schedule, error) {
			select {
			case polled <- struct{}{}:
			default:
			}
			return nil, nil
		},
	}

	s := New(ms, Config{
		RetryInterval:    5 * time.Millisecond,
		ScanInterval:     5 * time.Millisecond,
		TaskPollInterval: 5 * time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	s.Start(ctx)

	for _, ch := range []chan struct{}{requeued, scanned, polled} {
		select {
		case <-ch:
		case <-ctx.Done():
			t.Fatal("not all three scheduler components fired within timeout")
		}
	}

	cancel()
	s.Wait()
}
