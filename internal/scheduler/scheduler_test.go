// Whitebox tests: package scheduler (not scheduler_test) so we can reach the
// unexported retryScheduler and heartbeatScanner types directly.
package scheduler

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/peifengstudio/erminetq/internal/store"
)

// ── mock SchedulerStore ───────────────────────────────────────────────────────

type mockStore struct {
	mu sync.Mutex

	// Phase 3.1 hooks
	requeueRetryingFn   func(ctx context.Context) (int, error)
	findStaleAttemptsFn func(ctx context.Context, cutoff time.Time) ([]*store.Attempt, error)
	timeoutAttemptFn    func(ctx context.Context, id string) error

	// Phase 3.2 hooks
	dueSchedulesFn      func(ctx context.Context) ([]*store.Schedule, error)
	recordScheduleRunFn func(ctx context.Context, id string, nextRunAt time.Time) error
	createTaskFn        func(ctx context.Context, in store.CreateTaskInput) (*store.Task, error)
	listSchedulesFn     func(ctx context.Context) ([]*store.Schedule, error)
	updateScheduleFn    func(ctx context.Context, id string, in store.UpdateScheduleInput) (*store.Schedule, error)

	// Call counts (read under mu)
	requeueCalls   int
	findStaleCalls int
	timeoutCalls   int
}

func (m *mockStore) RequeueRetrying(ctx context.Context) (int, error) {
	m.mu.Lock()
	m.requeueCalls++
	m.mu.Unlock()
	if m.requeueRetryingFn != nil {
		return m.requeueRetryingFn(ctx)
	}
	return 0, nil
}

func (m *mockStore) FindStaleAttempts(ctx context.Context, cutoff time.Time) ([]*store.Attempt, error) {
	m.mu.Lock()
	m.findStaleCalls++
	m.mu.Unlock()
	if m.findStaleAttemptsFn != nil {
		return m.findStaleAttemptsFn(ctx, cutoff)
	}
	return nil, nil
}

func (m *mockStore) TimeoutAttempt(ctx context.Context, id string) error {
	m.mu.Lock()
	m.timeoutCalls++
	m.mu.Unlock()
	if m.timeoutAttemptFn != nil {
		return m.timeoutAttemptFn(ctx, id)
	}
	return nil
}

// ── Phase 3.2 configurable methods ───────────────────────────────────────────

// Add the function fields directly to the struct definition above; they are
// listed here for clarity.  All default to no-op returns so Phase 3.1 tests
// are unaffected.

func (m *mockStore) DueSchedules(ctx context.Context) ([]*store.Schedule, error) {
	if m.dueSchedulesFn != nil {
		return m.dueSchedulesFn(ctx)
	}
	return nil, nil
}

func (m *mockStore) RecordScheduleRun(ctx context.Context, id string, nextRunAt time.Time) error {
	if m.recordScheduleRunFn != nil {
		return m.recordScheduleRunFn(ctx, id, nextRunAt)
	}
	return nil
}

func (m *mockStore) CreateTask(ctx context.Context, in store.CreateTaskInput) (*store.Task, error) {
	if m.createTaskFn != nil {
		return m.createTaskFn(ctx, in)
	}
	return &store.Task{ID: "task-created"}, nil
}

func (m *mockStore) ListSchedules(ctx context.Context) ([]*store.Schedule, error) {
	if m.listSchedulesFn != nil {
		return m.listSchedulesFn(ctx)
	}
	return nil, nil
}

func (m *mockStore) UpdateSchedule(ctx context.Context, id string, in store.UpdateScheduleInput) (*store.Schedule, error) {
	if m.updateScheduleFn != nil {
		return m.updateScheduleFn(ctx, id, in)
	}
	return &store.Schedule{ID: id}, nil
}

// ── retryScheduler tests ──────────────────────────────────────────────────────

func TestRetryScheduler_CallsRequeueRetryingOnTick(t *testing.T) {
	called := make(chan struct{}, 1)
	ms := &mockStore{
		requeueRetryingFn: func(_ context.Context) (int, error) {
			select {
			case called <- struct{}{}:
			default:
			}
			return 2, nil
		},
	}

	r := newRetryScheduler(ms, 5*time.Millisecond, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	r.start(ctx)

	select {
	case <-called:
		// RequeueRetrying fired
	case <-ctx.Done():
		t.Fatal("RequeueRetrying was not called within timeout")
	}

	cancel()
	r.wait()
}

func TestRetryScheduler_StopsOnCtxCancel(t *testing.T) {
	ms := &mockStore{}
	r := newRetryScheduler(ms, 10*time.Millisecond, nil)

	ctx, cancel := context.WithCancel(context.Background())
	r.start(ctx)
	cancel()

	done := make(chan struct{})
	go func() { r.wait(); close(done) }()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("retryScheduler did not stop after ctx cancellation")
	}
}

func TestRetryScheduler_ErrorForwardedToOnError(t *testing.T) {
	boom := errors.New("db unavailable")
	errCh := make(chan error, 1)
	onError := func(err error) {
		select {
		case errCh <- err:
		default:
		}
	}

	ms := &mockStore{
		requeueRetryingFn: func(_ context.Context) (int, error) {
			return 0, boom
		},
	}

	r := newRetryScheduler(ms, 5*time.Millisecond, onError)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	r.start(ctx)

	select {
	case err := <-errCh:
		if !errors.Is(err, boom) {
			t.Errorf("onError received %v, want %v", err, boom)
		}
	case <-ctx.Done():
		t.Fatal("onError was not called after RequeueRetrying failed")
	}

	cancel()
	r.wait()
}

func TestRetryScheduler_ErrorDoesNotPanic(t *testing.T) {
	// With nil onError, errors must be swallowed silently.
	ms := &mockStore{
		requeueRetryingFn: func(_ context.Context) (int, error) {
			return 0, errors.New("silent error")
		},
	}

	r := newRetryScheduler(ms, 5*time.Millisecond, nil /* no handler */)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// If this does not panic, the test passes.
	r.start(ctx)
	r.wait()
}

// ── heartbeatScanner tests ────────────────────────────────────────────────────

func TestHeartbeatScanner_TimesOutStaleAttempts(t *testing.T) {
	stale := []*store.Attempt{{ID: "attempt-stale-1"}, {ID: "attempt-stale-2"}}
	timedOut := make(chan string, len(stale))

	ms := &mockStore{
		findStaleAttemptsFn: func(_ context.Context, _ time.Time) ([]*store.Attempt, error) {
			return stale, nil
		},
		timeoutAttemptFn: func(_ context.Context, id string) error {
			timedOut <- id
			return nil
		},
	}

	h := newHeartbeatScanner(ms, 5*time.Millisecond, 60*time.Second, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	h.start(ctx)

	// Collect the first two timeouts.
	got := make([]string, 0, len(stale))
	for range stale {
		select {
		case id := <-timedOut:
			got = append(got, id)
		case <-ctx.Done():
			t.Fatalf("only %d/%d attempts timed out before timeout", len(got), len(stale))
		}
	}

	cancel()
	h.wait()

	for i, a := range stale {
		if got[i] != a.ID {
			t.Errorf("timedOut[%d] = %q, want %q", i, got[i], a.ID)
		}
	}
}

func TestHeartbeatScanner_EmptyStaleList(t *testing.T) {
	// FindStaleAttempts returns nothing — TimeoutAttempt must never be called.
	var timeoutCalls atomic.Int32
	ms := &mockStore{
		findStaleAttemptsFn: func(_ context.Context, _ time.Time) ([]*store.Attempt, error) {
			return nil, nil
		},
		timeoutAttemptFn: func(_ context.Context, _ string) error {
			timeoutCalls.Add(1)
			return nil
		},
	}

	h := newHeartbeatScanner(ms, 5*time.Millisecond, 60*time.Second, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()

	h.start(ctx)
	h.wait()

	if n := timeoutCalls.Load(); n != 0 {
		t.Errorf("TimeoutAttempt called %d times, want 0 (no stale attempts)", n)
	}
}

func TestHeartbeatScanner_CutoffPassedCorrectly(t *testing.T) {
	const hbCutoff = 60 * time.Second
	cutoffCh := make(chan time.Time, 1)

	ms := &mockStore{
		findStaleAttemptsFn: func(_ context.Context, cutoff time.Time) ([]*store.Attempt, error) {
			select {
			case cutoffCh <- cutoff:
			default:
			}
			return nil, nil
		},
	}

	h := newHeartbeatScanner(ms, 5*time.Millisecond, hbCutoff, nil)

	before := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	h.start(ctx)

	var received time.Time
	select {
	case received = <-cutoffCh:
	case <-ctx.Done():
		t.Fatal("FindStaleAttempts was not called")
	}

	after := time.Now()
	cancel()
	h.wait()

	// received must be in [before − hbCutoff, after − hbCutoff].
	lo := before.Add(-hbCutoff)
	hi := after.Add(-hbCutoff)
	if received.Before(lo) || received.After(hi) {
		t.Errorf("cutoff = %v, want in [%v, %v]", received, lo, hi)
	}
}

func TestHeartbeatScanner_FindStaleError_CallsOnError(t *testing.T) {
	boom := errors.New("db read failed")
	errCh := make(chan error, 1)

	ms := &mockStore{
		findStaleAttemptsFn: func(_ context.Context, _ time.Time) ([]*store.Attempt, error) {
			return nil, boom
		},
	}

	h := newHeartbeatScanner(ms, 5*time.Millisecond, 60*time.Second, func(err error) {
		select {
		case errCh <- err:
		default:
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	h.start(ctx)

	select {
	case err := <-errCh:
		if !errors.Is(err, boom) {
			t.Errorf("onError got %v, want %v", err, boom)
		}
	case <-ctx.Done():
		t.Fatal("onError was not called after FindStaleAttempts failed")
	}

	cancel()
	h.wait()
}

func TestHeartbeatScanner_TimeoutAttemptError_ContinuesOtherAttempts(t *testing.T) {
	// Two stale attempts; the first TimeoutAttempt fails.
	// The scanner must still call TimeoutAttempt for the second attempt.
	stale := []*store.Attempt{{ID: "a1"}, {ID: "a2"}}
	var timedOut []string
	var mu sync.Mutex
	var errCount atomic.Int32

	ms := &mockStore{
		findStaleAttemptsFn: func(_ context.Context, _ time.Time) ([]*store.Attempt, error) {
			return stale, nil
		},
		timeoutAttemptFn: func(_ context.Context, id string) error {
			mu.Lock()
			timedOut = append(timedOut, id)
			mu.Unlock()
			if id == "a1" {
				return errors.New("transient error")
			}
			return nil
		},
	}

	h := newHeartbeatScanner(ms, 5*time.Millisecond, 60*time.Second, func(_ error) {
		errCount.Add(1)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	h.start(ctx)

	// Wait until a2 is processed.
	deadline := time.Now().Add(400 * time.Millisecond)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(timedOut)
		mu.Unlock()
		if n >= 2 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	cancel()
	h.wait()

	mu.Lock()
	got := timedOut
	mu.Unlock()

	// Both attempts should have been attempted regardless of the first error.
	found := map[string]bool{}
	for _, id := range got {
		found[id] = true
	}
	if !found["a1"] || !found["a2"] {
		t.Errorf("timedOut = %v, want both a1 and a2", got)
	}
	if errCount.Load() == 0 {
		t.Error("onError should have been called for the a1 TimeoutAttempt failure")
	}
}

func TestHeartbeatScanner_StopsOnCtxCancel(t *testing.T) {
	ms := &mockStore{}
	h := newHeartbeatScanner(ms, 10*time.Millisecond, 60*time.Second, nil)

	ctx, cancel := context.WithCancel(context.Background())
	h.start(ctx)
	cancel()

	done := make(chan struct{})
	go func() { h.wait(); close(done) }()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("heartbeatScanner did not stop after ctx cancellation")
	}
}

// ── Scheduler coordinator tests ───────────────────────────────────────────────

func TestScheduler_StartWait_CleanShutdown(t *testing.T) {
	ms := &mockStore{}
	s := New(ms, Config{
		RetryInterval: 10 * time.Millisecond,
		ScanInterval:  10 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	s.Start(ctx)
	cancel()

	done := make(chan struct{})
	go func() { s.Wait(); close(done) }()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Scheduler.Wait() did not return after ctx cancellation")
	}
}

func TestScheduler_BothComponentsRun(t *testing.T) {
	requeued := make(chan struct{}, 1)
	scanned := make(chan struct{}, 1)

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
	}

	s := New(ms, Config{
		RetryInterval: 5 * time.Millisecond,
		ScanInterval:  5 * time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	s.Start(ctx)

	for _, ch := range []chan struct{}{requeued, scanned} {
		select {
		case <-ch:
		case <-ctx.Done():
			t.Fatal("not all scheduler components fired within timeout")
		}
	}

	cancel()
	s.Wait()
}

func TestScheduler_DefaultsApplied(t *testing.T) {
	// Passing a zero Config must not panic or use zero-value durations.
	ms := &mockStore{}
	s := New(ms, Config{}) // all zeros

	ctx, cancel := context.WithCancel(context.Background())
	s.Start(ctx)
	cancel()
	s.Wait() // must not hang
}

func TestScheduler_OnErrorPropagated(t *testing.T) {
	// OnError from Config must reach both sub-schedulers.
	boom := errors.New("store error")
	errCh := make(chan error, 2)

	ms := &mockStore{
		requeueRetryingFn: func(_ context.Context) (int, error) {
			return 0, boom
		},
		findStaleAttemptsFn: func(_ context.Context, _ time.Time) ([]*store.Attempt, error) {
			return nil, boom
		},
	}

	s := New(ms, Config{
		RetryInterval: 5 * time.Millisecond,
		ScanInterval:  5 * time.Millisecond,
		OnError: func(err error) {
			select {
			case errCh <- err:
			default:
			}
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	s.Start(ctx)

	// Expect at least two errors (one from each component).
	received := 0
	for received < 2 {
		select {
		case <-errCh:
			received++
		case <-ctx.Done():
			t.Fatalf("only %d/2 error callbacks received before timeout", received)
		}
	}

	cancel()
	s.Wait()
}
