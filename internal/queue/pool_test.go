package queue_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/peifengstudio/erminetq/internal/queue"
	"github.com/peifengstudio/erminetq/internal/store"
)

// ── helpers ───────────────────────────────────────────────────────────────────

// taskSource returns a ClaimTask func that dispenses at most n tasks and then
// always returns (nil, "", nil).  Each task gets a unique attempt ID.
func taskSource(n int) func() (*store.Task, string, error) {
	remaining := int64(n)
	var counter int64
	return func() (*store.Task, string, error) {
		if atomic.AddInt64(&remaining, -1) < 0 {
			return nil, "", nil
		}
		id := fmt.Sprintf("attempt-%d", atomic.AddInt64(&counter, 1))
		return defaultTask(), id, nil
	}
}

// newPoolCfg builds a PoolConfig wired to ms and a registry with "job"→fn.
// Defaults: concurrency=2, poll interval=10ms (fast for tests).
func newPoolCfg(ms *mockStore, fn queue.WorkerFunc) queue.PoolConfig {
	r := queue.NewRegistry()
	r.Register("job", fn)
	return queue.PoolConfig{
		Store:        ms,
		Registry:     r,
		Config:       unlimitedConfig(),
		Queue:        "default",
		Concurrency:  2,
		PollInterval: 10 * time.Millisecond,
	}
}

// ── tests ─────────────────────────────────────────────────────────────────────

func TestPool_StartStop(t *testing.T) {
	// Pool with an always-empty queue: Start → cancel → Wait must not hang.
	ms := &mockStore{claimTask: func() (*store.Task, string, error) { return nil, "", nil }}
	cfg := newPoolCfg(ms, nopWorker)

	p, err := queue.NewPool(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx)
	cancel()

	done := make(chan struct{})
	go func() { p.Wait(); close(done) }()

	select {
	case <-done:
		// clean exit
	case <-time.After(2 * time.Second):
		t.Fatal("Wait() did not return after ctx cancellation")
	}
}

func TestPool_ProcessesAllTasks(t *testing.T) {
	const numTasks = 6
	ms := &mockStore{claimTask: taskSource(numTasks)}

	allDone := make(chan struct{})
	var processed atomic.Int32
	ms.onSucceed = func(_ json.RawMessage) error {
		if processed.Add(1) == numTasks {
			close(allDone)
		}
		return nil
	}

	cfg := newPoolCfg(ms, func(_ context.Context, _ []byte) ([]byte, error) {
		return nil, nil
	})

	p, err := queue.NewPool(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	p.Start(ctx)

	select {
	case <-allDone:
		// all tasks processed
	case <-ctx.Done():
		t.Fatalf("timed out; only %d/%d tasks processed", processed.Load(), numTasks)
	}

	cancel()
	p.Wait()
}

func TestPool_ConcurrentExecution(t *testing.T) {
	// Barrier test: with concurrency=3, all 3 tasks must start before any can
	// finish.  If the pool had fewer goroutines the barrier would deadlock and
	// the test context would time out.
	const concurrency = 3
	ms := &mockStore{claimTask: taskSource(concurrency)}

	var started atomic.Int32
	proceed := make(chan struct{})

	cfg := newPoolCfg(ms, func(_ context.Context, _ []byte) ([]byte, error) {
		if started.Add(1) == int32(concurrency) {
			close(proceed) // last one to start opens the gate
		}
		<-proceed
		return nil, nil
	})
	cfg.Concurrency = concurrency

	p, err := queue.NewPool(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	p.Start(ctx)

	select {
	case <-proceed:
		// all concurrency goroutines ran in parallel
	case <-ctx.Done():
		t.Fatalf("barrier not released: only %d/%d tasks started concurrently",
			started.Load(), concurrency)
	}

	cancel()
	p.Wait()
}

func TestPool_WaitBlocksUntilInFlightComplete(t *testing.T) {
	// Verifies that Wait() does not return while a task is still executing.
	// The WorkerFunc intentionally does NOT respect ctx so it acts as a
	// slow/non-cooperative in-flight task.
	ms := &mockStore{claimTask: taskSource(1)}

	unblock := make(chan struct{})
	started := make(chan struct{}, 1)

	cfg := newPoolCfg(ms, func(_ context.Context, _ []byte) ([]byte, error) {
		started <- struct{}{} // signal: fn is running
		<-unblock             // block until released (ignores ctx on purpose)
		return nil, nil
	})
	cfg.Concurrency = 1

	p, err := queue.NewPool(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx)

	<-started // fn is now running
	cancel()  // shut down the pool

	// Wait() must not return while fn is still blocked.
	waitReturned := make(chan struct{})
	go func() { p.Wait(); close(waitReturned) }()

	select {
	case <-waitReturned:
		t.Error("Wait() returned before in-flight task completed")
	case <-time.After(40 * time.Millisecond):
		// expected: still blocked
	}

	// Release the task; Wait() should now complete.
	close(unblock)

	select {
	case <-waitReturned:
		// clean
	case <-time.After(2 * time.Second):
		t.Error("Wait() did not return after in-flight task completed")
	}
}

func TestPool_WorkerID(t *testing.T) {
	const wantID = "w-pool-test"
	ms := &mockStore{
		claimTask: func() (*store.Task, string, error) { return nil, "", nil },
		createWorkerFn: func() (*store.Worker, error) {
			return &store.Worker{ID: wantID}, nil
		},
	}
	cfg := newPoolCfg(ms, nopWorker)

	p, err := queue.NewPool(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	if p.WorkerID() != wantID {
		t.Errorf("WorkerID = %q, want %q", p.WorkerID(), wantID)
	}
}

func TestPool_PollIntervalBackoff(t *testing.T) {
	// With an always-empty queue and a 30ms poll interval, ClaimTask should be
	// called at most ~5 times in 100ms (1 initial + ~3 polls).
	// This catches accidental busy-loop regressions.
	var claimCount atomic.Int32
	ms := &mockStore{
		claimTask: func() (*store.Task, string, error) {
			claimCount.Add(1)
			return nil, "", nil
		},
	}
	cfg := newPoolCfg(ms, nopWorker)
	cfg.Concurrency = 1
	cfg.PollInterval = 30 * time.Millisecond

	p, err := queue.NewPool(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	p.Start(ctx)
	p.Wait()

	n := claimCount.Load()
	if n < 1 {
		t.Error("ClaimTask was never called")
	}
	// With 30ms interval over 100ms, expect ≤ 5 calls (generous upper bound).
	// A busy-loop would produce hundreds.
	if n > 5 {
		t.Errorf("ClaimTask called %d times in 100ms with 30ms poll interval — suspected busy-loop", n)
	}
}

func TestPool_OnErrorCallback(t *testing.T) {
	// When RunOnce returns a non-ctx error, OnError should be called.
	// We trigger this by making SucceedAttempt fail.
	ms := &mockStore{
		claimTask: taskSource(1),
		onSucceed: func(_ json.RawMessage) error { return fmt.Errorf("disk full") },
	}

	var mu sync.Mutex
	var errs []error
	cfg := newPoolCfg(ms, func(_ context.Context, _ []byte) ([]byte, error) {
		return nil, nil
	})
	cfg.Concurrency = 1
	cfg.OnError = func(err error) {
		mu.Lock()
		errs = append(errs, err)
		mu.Unlock()
	}

	p, err := queue.NewPool(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}

	// Give it enough time for the error path to trigger.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	p.Start(ctx)

	// Wait for OnError to fire (or timeout).
	deadline := time.Now().Add(400 * time.Millisecond)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(errs)
		mu.Unlock()
		if n > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	p.Wait()

	mu.Lock()
	n := len(errs)
	mu.Unlock()
	if n == 0 {
		t.Error("OnError was never called despite SucceedAttempt returning an error")
	}
}

func TestPool_NewPoolCreateWorkerError(t *testing.T) {
	// NewPool must propagate CreateWorker errors.
	ms := &mockStore{
		createWorkerFn: func() (*store.Worker, error) {
			return nil, fmt.Errorf("db unavailable")
		},
	}
	cfg := newPoolCfg(ms, nopWorker)

	_, err := queue.NewPool(context.Background(), cfg)
	if err == nil {
		t.Error("NewPool: expected error when CreateWorker fails, got nil")
	}
}

func TestPool_ConcurrencyDefaultsToOne(t *testing.T) {
	// Concurrency <= 0 should be clamped to 1 (not panic or spawn 0 goroutines).
	ms := &mockStore{claimTask: func() (*store.Task, string, error) { return nil, "", nil }}
	cfg := newPoolCfg(ms, nopWorker)
	cfg.Concurrency = 0

	p, err := queue.NewPool(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx)
	cancel()

	done := make(chan struct{})
	go func() { p.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Wait() hung with Concurrency=0 (should default to 1)")
	}
}
