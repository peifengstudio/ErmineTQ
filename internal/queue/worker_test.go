package queue_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/peifengstudio/erminetq/internal/bridge"
	"github.com/peifengstudio/erminetq/internal/config"
	"github.com/peifengstudio/erminetq/internal/queue"
	"github.com/peifengstudio/erminetq/internal/store"
)

// ── mock store ────────────────────────────────────────────────────────────────

// mockStore satisfies queue.TaskStore.  By default ClaimTask returns a
// standard running task and CreateWorker returns a worker with a fixed ID.
// Every outcome method records what it was called with and succeeds.
// Override individual fields to inject specific behaviour.
type mockStore struct {
	mu sync.Mutex

	// Configurable behaviours
	createWorkerFn func() (*store.Worker, error)
	claimTask      func() (*store.Task, string, error)
	onSucceed      func(result json.RawMessage) error
	onFail         func(errMsg string) error
	onCancel       func() error
	onHeartbeat    func() error

	// Observations
	heartbeatCount int64 // accessed atomically
	lastFailMsg    string
	lastResult     json.RawMessage
	succeededCalls int
	failedCalls    int
	cancelledCalls int
}

func (m *mockStore) CreateWorker(_ context.Context, _ store.CreateWorkerInput) (*store.Worker, error) {
	if m.createWorkerFn != nil {
		return m.createWorkerFn()
	}
	return &store.Worker{ID: "w-mock"}, nil
}

func (m *mockStore) ClaimTask(_ context.Context, _, _ string, _ []string, _ *config.Config) (*store.Task, string, error) {
	if m.claimTask != nil {
		return m.claimTask()
	}
	return defaultTask(), "attempt-1", nil
}

func (m *mockStore) SucceedAttempt(_ context.Context, _ string, result json.RawMessage) error {
	m.mu.Lock()
	m.lastResult = result
	m.succeededCalls++
	m.mu.Unlock()
	if m.onSucceed != nil {
		return m.onSucceed(result)
	}
	return nil
}

func (m *mockStore) FailAttempt(_ context.Context, _ string, errMsg string) error {
	m.mu.Lock()
	m.lastFailMsg = errMsg
	m.failedCalls++
	m.mu.Unlock()
	if m.onFail != nil {
		return m.onFail(errMsg)
	}
	return nil
}

func (m *mockStore) CancelAttempt(_ context.Context, _ string) error {
	m.mu.Lock()
	m.cancelledCalls++
	m.mu.Unlock()
	if m.onCancel != nil {
		return m.onCancel()
	}
	return nil
}

func (m *mockStore) UpdateHeartbeat(_ context.Context, _ string) error {
	atomic.AddInt64(&m.heartbeatCount, 1)
	if m.onHeartbeat != nil {
		return m.onHeartbeat()
	}
	return nil
}

// ── helpers ───────────────────────────────────────────────────────────────────

// defaultTask returns a minimal task suitable for most tests.
func defaultTask() *store.Task {
	return &store.Task{
		ID:     "task-1",
		Type:   "job",
		Queue:  "default",
		Status: store.TaskStatusRunning,
	}
}

// unlimitedConfig returns a config that imposes no concurrency limits.
func unlimitedConfig() *config.Config {
	return &config.Config{
		Limits:    config.LimitsConfig{Global: 10_000},
		Queues:    map[string]config.QueueConfig{},
		TaskTypes: map[string]config.TaskTypeConfig{},
	}
}

// newCfg builds a RunConfig wired to ms and a registry with "job" → fn.
func newCfg(ms *mockStore, fn queue.WorkerFunc) queue.RunConfig {
	r := queue.NewRegistry()
	r.Register("job", fn)
	return queue.RunConfig{
		Store:             ms,
		Registry:          r,
		WorkerID:          "w-1",
		Queue:             "default",
		Config:            unlimitedConfig(),
		HeartbeatInterval: time.Hour, // disabled by default; override per test
	}
}

// ── RunOnce tests ─────────────────────────────────────────────────────────────

func TestRunOnce_Success(t *testing.T) {
	ms := &mockStore{}
	result := json.RawMessage(`{"rows":7}`)
	cfg := newCfg(ms, func(_ context.Context, _ []byte) ([]byte, error) {
		return result, nil
	})

	got, err := queue.RunOnce(context.Background(), cfg)
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !got {
		t.Error("returned false, want true (task was executed)")
	}
	if ms.succeededCalls != 1 {
		t.Errorf("SucceedAttempt calls = %d, want 1", ms.succeededCalls)
	}
	if string(ms.lastResult) != string(result) {
		t.Errorf("result = %s, want %s", ms.lastResult, result)
	}
	if ms.failedCalls != 0 || ms.cancelledCalls != 0 {
		t.Errorf("unexpected fail=%d cancel=%d calls", ms.failedCalls, ms.cancelledCalls)
	}
}

func TestRunOnce_WorkerFuncError(t *testing.T) {
	ms := &mockStore{}
	cfg := newCfg(ms, func(_ context.Context, _ []byte) ([]byte, error) {
		return nil, errors.New("something broke")
	})

	got, err := queue.RunOnce(context.Background(), cfg)
	if err != nil {
		t.Fatalf("RunOnce returned infrastructure error: %v", err)
	}
	if !got {
		t.Error("returned false, want true (task was processed, even though it failed)")
	}
	if ms.failedCalls != 1 {
		t.Errorf("FailAttempt calls = %d, want 1", ms.failedCalls)
	}
	if ms.lastFailMsg != "something broke" {
		t.Errorf("fail message = %q, want %q", ms.lastFailMsg, "something broke")
	}
	if ms.succeededCalls != 0 {
		t.Errorf("SucceedAttempt called %d times, want 0", ms.succeededCalls)
	}
}

func TestRunOnce_QueueEmpty(t *testing.T) {
	ms := &mockStore{
		claimTask: func() (*store.Task, string, error) { return nil, "", nil },
	}
	cfg := newCfg(ms, nopWorker)

	got, err := queue.RunOnce(context.Background(), cfg)
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if got {
		t.Error("returned true, want false (queue is empty)")
	}
	if ms.succeededCalls+ms.failedCalls+ms.cancelledCalls != 0 {
		t.Error("no outcome methods should be called when queue is empty")
	}
}

func TestRunOnce_ClaimError(t *testing.T) {
	claimErr := errors.New("db unavailable")
	ms := &mockStore{
		claimTask: func() (*store.Task, string, error) { return nil, "", claimErr },
	}
	cfg := newCfg(ms, nopWorker)

	got, err := queue.RunOnce(context.Background(), cfg)
	if got {
		t.Error("returned true, want false on claim error")
	}
	if !errors.Is(err, claimErr) {
		t.Errorf("err = %v, want wrapping %v", err, claimErr)
	}
}

func TestRunOnce_EmptyRegistry(t *testing.T) {
	ms := &mockStore{}
	cfg := queue.RunConfig{
		Store:    ms,
		Registry: queue.NewRegistry(), // nothing registered
		WorkerID: "w-1",
		Queue:    "default",
		Config:   unlimitedConfig(),
	}

	got, err := queue.RunOnce(context.Background(), cfg)
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if got {
		t.Error("returned true, want false (registry is empty)")
	}
}

func TestRunOnce_ParentContextCancelled(t *testing.T) {
	ms := &mockStore{}
	started := make(chan struct{})
	cfg := newCfg(ms, func(ctx context.Context, _ []byte) ([]byte, error) {
		close(started)
		<-ctx.Done() // block until context is cancelled
		return nil, ctx.Err()
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var (
		gotBool bool
		gotErr  error
	)
	go func() {
		defer close(done)
		gotBool, gotErr = queue.RunOnce(ctx, cfg)
	}()

	<-started // fn is running
	cancel()  // send halt signal
	<-done

	if gotBool {
		t.Error("returned true, want false (ctx was cancelled)")
	}
	if !errors.Is(gotErr, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", gotErr)
	}
	if ms.cancelledCalls != 1 {
		t.Errorf("CancelAttempt calls = %d, want 1", ms.cancelledCalls)
	}
	if ms.succeededCalls != 0 || ms.failedCalls != 0 {
		t.Errorf("unexpected succeed=%d fail=%d", ms.succeededCalls, ms.failedCalls)
	}
}

func TestRunOnce_PanicRecovery(t *testing.T) {
	ms := &mockStore{}
	cfg := newCfg(ms, func(_ context.Context, _ []byte) ([]byte, error) {
		panic("oops, catastrophic failure")
	})

	got, err := queue.RunOnce(context.Background(), cfg)
	if err != nil {
		t.Fatalf("RunOnce returned infrastructure error after panic: %v", err)
	}
	if !got {
		t.Error("returned false, want true (panic counts as executed)")
	}
	if ms.failedCalls != 1 {
		t.Errorf("FailAttempt calls = %d, want 1", ms.failedCalls)
	}
	const wantPrefix = "panic: oops, catastrophic failure"
	if ms.lastFailMsg != wantPrefix {
		t.Errorf("fail message = %q, want %q", ms.lastFailMsg, wantPrefix)
	}
}

func TestRunOnce_PanicWithNonStringValue(t *testing.T) {
	ms := &mockStore{}
	cfg := newCfg(ms, func(_ context.Context, _ []byte) ([]byte, error) {
		panic(42) // non-string panic value
	})

	_, err := queue.RunOnce(context.Background(), cfg)
	if err != nil {
		t.Fatalf("unexpected infra error: %v", err)
	}
	if ms.failedCalls != 1 {
		t.Errorf("FailAttempt calls = %d, want 1", ms.failedCalls)
	}
	if ms.lastFailMsg == "" {
		t.Error("fail message should not be empty for non-string panic")
	}
}

func TestRunOnce_HeartbeatFires(t *testing.T) {
	ms := &mockStore{}
	cfg := newCfg(ms, func(_ context.Context, _ []byte) ([]byte, error) {
		time.Sleep(35 * time.Millisecond) // long enough for ~3 heartbeats at 10ms
		return nil, nil
	})
	cfg.HeartbeatInterval = 10 * time.Millisecond

	got, err := queue.RunOnce(context.Background(), cfg)
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !got {
		t.Error("returned false, want true")
	}

	hbCount := atomic.LoadInt64(&ms.heartbeatCount)
	if hbCount < 2 {
		t.Errorf("UpdateHeartbeat called %d times, want >= 2", hbCount)
	}
}

func TestRunOnce_HeartbeatStopsAfterExecution(t *testing.T) {
	// The heartbeat goroutine must not call UpdateHeartbeat after the outcome
	// has been written.  We verify this by checking the count stays stable
	// after RunOnce returns.
	ms := &mockStore{}
	cfg := newCfg(ms, func(_ context.Context, _ []byte) ([]byte, error) {
		time.Sleep(15 * time.Millisecond)
		return nil, nil
	})
	cfg.HeartbeatInterval = 5 * time.Millisecond

	if _, err := queue.RunOnce(context.Background(), cfg); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	countAfter := atomic.LoadInt64(&ms.heartbeatCount)
	time.Sleep(20 * time.Millisecond)
	countLater := atomic.LoadInt64(&ms.heartbeatCount)

	if countLater != countAfter {
		t.Errorf("heartbeat continued after RunOnce returned: count rose from %d to %d",
			countAfter, countLater)
	}
}

func TestRunOnce_PayloadForwardedToFunc(t *testing.T) {
	payload := []byte(`{"limit":100}`)
	ms := &mockStore{
		claimTask: func() (*store.Task, string, error) {
			t := defaultTask()
			t.Payload = payload
			return t, "attempt-1", nil
		},
	}

	var received []byte
	cfg := newCfg(ms, func(_ context.Context, p []byte) ([]byte, error) {
		received = p
		return nil, nil
	})

	if _, err := queue.RunOnce(context.Background(), cfg); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if string(received) != string(payload) {
		t.Errorf("fn received payload %q, want %q", received, payload)
	}
}

func TestRunOnce_TimeoutSecsCreatesDeadlineCtx(t *testing.T) {
	// WorkerFunc receives a context with a 1s deadline; fn honours it and
	// returns DeadlineExceeded → FailAttempt should be called.
	timeoutSecs := 1
	ms := &mockStore{
		claimTask: func() (*store.Task, string, error) {
			tk := defaultTask()
			tk.TimeoutSecs = &timeoutSecs
			return tk, "attempt-1", nil
		},
	}
	cfg := newCfg(ms, func(ctx context.Context, _ []byte) ([]byte, error) {
		// Check that the context carries a deadline.
		if _, ok := ctx.Deadline(); !ok {
			return nil, errors.New("expected a deadline on exec ctx")
		}
		return []byte(`"ok"`), nil
	})

	got, err := queue.RunOnce(context.Background(), cfg)
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !got {
		t.Error("want true")
	}
	if ms.succeededCalls != 1 {
		t.Errorf("SucceedAttempt calls = %d, want 1", ms.succeededCalls)
	}
}

func TestRunOnce_SucceedAttemptError(t *testing.T) {
	// If SucceedAttempt itself returns an error, RunOnce propagates it.
	storeErr := errors.New("disk full")
	ms := &mockStore{
		onSucceed: func(_ json.RawMessage) error { return storeErr },
	}
	cfg := newCfg(ms, func(_ context.Context, _ []byte) ([]byte, error) {
		return []byte(`"ok"`), nil
	})

	got, err := queue.RunOnce(context.Background(), cfg)
	if got {
		t.Error("returned true, want false on store error")
	}
	if !errors.Is(err, storeErr) {
		t.Errorf("err = %v, want wrapping %v", err, storeErr)
	}
}

// ── Bridge dispatch via RunOnce ────────────────────────────────────────────────

// startSuccessBridge starts a Unix socket server that always returns success.
func startSuccessBridge(t *testing.T, resultJSON string) string {
	t.Helper()
	sockPath := shortSock(t, "wb.sock")
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("bridge listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				sc := bufio.NewScanner(c)
				for sc.Scan() {
					resp, _ := json.Marshal(map[string]json.RawMessage{
						"result": json.RawMessage(resultJSON),
					})
					resp = append(resp, '\n')
					_, _ = c.Write(resp)
				}
			}(conn)
		}
	}()
	return sockPath
}

// newBridgeCfg builds a RunConfig where "py_job" is routed to a bridge client.
func newBridgeCfg(ms *mockStore, sockPath string) queue.RunConfig {
	r := queue.NewRegistry()
	c := bridge.NewClient(sockPath)
	r.SetBridge(c, []string{"py_job"})
	return queue.RunConfig{
		Store:             ms,
		Registry:          r,
		WorkerID:          "w-bridge",
		Queue:             "default",
		Config:            unlimitedConfig(),
		HeartbeatInterval: time.Hour,
	}
}

func TestRunOnce_BridgeType_Success(t *testing.T) {
	wantResult := `{"rows":42}`
	sockPath := startSuccessBridge(t, wantResult)

	ms := &mockStore{
		claimTask: func() (*store.Task, string, error) {
			return &store.Task{
				ID: "t-bridge", Type: "py_job", Payload: []byte(`{}`),
			}, "attempt-bridge", nil
		},
	}
	cfg := newBridgeCfg(ms, sockPath)

	got, err := queue.RunOnce(context.Background(), cfg)
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !got {
		t.Error("got=false, want true")
	}
	if ms.succeededCalls != 1 {
		t.Errorf("SucceedAttempt called %d times, want 1", ms.succeededCalls)
	}
	if string(ms.lastResult) != wantResult {
		t.Errorf("result = %q, want %q", ms.lastResult, wantResult)
	}
}

func TestRunOnce_BridgeType_Error(t *testing.T) {
	sockPath := shortSock(t, "eb.sock")
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				sc := bufio.NewScanner(c)
				for sc.Scan() {
					resp, _ := json.Marshal(map[string]string{"error": "python crashed"})
					resp = append(resp, '\n')
					_, _ = c.Write(resp)
				}
			}(conn)
		}
	}()

	ms := &mockStore{
		claimTask: func() (*store.Task, string, error) {
			return &store.Task{ID: "t-err", Type: "py_job", Payload: []byte(`{}`)},
				"attempt-err", nil
		},
	}
	cfg := newBridgeCfg(ms, sockPath)

	got, err := queue.RunOnce(context.Background(), cfg)
	if err != nil {
		t.Fatalf("RunOnce infrastructure error: %v", err)
	}
	if !got {
		t.Error("got=false, want true (failure is handled, not infra error)")
	}
	if ms.failedCalls != 1 {
		t.Errorf("FailAttempt called %d times, want 1", ms.failedCalls)
	}
	if ms.lastFailMsg != "python crashed" {
		t.Errorf("fail message = %q, want \"python crashed\"", ms.lastFailMsg)
	}
}
func TestRunOnce_FailAttemptError(t *testing.T) {
	storeErr := errors.New("write timeout")
	ms := &mockStore{
		onFail: func(_ string) error { return storeErr },
	}
	cfg := newCfg(ms, func(_ context.Context, _ []byte) ([]byte, error) {
		return nil, errors.New("worker error")
	})

	got, err := queue.RunOnce(context.Background(), cfg)
	if got {
		t.Error("returned true, want false on store error")
	}
	if !errors.Is(err, storeErr) {
		t.Errorf("err = %v, want wrapping %v", err, storeErr)
	}
}
