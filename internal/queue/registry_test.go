package queue_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/peifengstudio/erminetq/internal/bridge"
	"github.com/peifengstudio/erminetq/internal/queue"
	"github.com/peifengstudio/erminetq/internal/store"
)

// nopWorker is a trivial WorkerFunc used as a stand-in wherever the function
// body doesn't matter, only its identity.
func nopWorker(_ context.Context, _ []byte) ([]byte, error) { return nil, nil }

// ── Register / Lookup ─────────────────────────────────────────────────────────

func TestRegistry_RegisterAndLookup(t *testing.T) {
	r := queue.NewRegistry()
	r.Register("crawl", nopWorker)

	fn, ok := r.Lookup("crawl")
	if !ok {
		t.Fatal("Lookup: expected ok=true, got false")
	}
	if fn == nil {
		t.Fatal("Lookup: returned nil WorkerFunc")
	}
}

func TestRegistry_LookupUnknownType(t *testing.T) {
	r := queue.NewRegistry()

	fn, ok := r.Lookup("nonexistent")
	if ok {
		t.Error("Lookup: expected ok=false for unknown type, got true")
	}
	if fn != nil {
		t.Error("Lookup: expected nil WorkerFunc for unknown type")
	}
}

func TestRegistry_RegisterOverwritesPrevious(t *testing.T) {
	r := queue.NewRegistry()

	var called string
	first := func(_ context.Context, _ []byte) ([]byte, error) {
		called = "first"
		return nil, nil
	}
	second := func(_ context.Context, _ []byte) ([]byte, error) {
		called = "second"
		return nil, nil
	}

	r.Register("job", first)
	r.Register("job", second) // overwrites

	fn, ok := r.Lookup("job")
	if !ok {
		t.Fatal("Lookup after overwrite: expected ok=true")
	}
	fn(context.Background(), nil) //nolint:errcheck
	if called != "second" {
		t.Errorf("called = %q, want %q (second registration should win)", called, "second")
	}
}

func TestRegistry_RegisterNilPanics(t *testing.T) {
	r := queue.NewRegistry()
	defer func() {
		if recover() == nil {
			t.Error("Register(nil) should panic")
		}
	}()
	r.Register("job", nil)
}

// ── TaskTypes ─────────────────────────────────────────────────────────────────

func TestRegistry_TaskTypesEmpty(t *testing.T) {
	r := queue.NewRegistry()
	types := r.TaskTypes()
	if len(types) != 0 {
		t.Errorf("TaskTypes on empty registry = %v, want []", types)
	}
}

func TestRegistry_TaskTypesSorted(t *testing.T) {
	r := queue.NewRegistry()
	r.Register("zebra", nopWorker)
	r.Register("apple", nopWorker)
	r.Register("mango", nopWorker)

	got := r.TaskTypes()
	want := []string{"apple", "mango", "zebra"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("TaskTypes = %v, want %v", got, want)
	}
}

func TestRegistry_TaskTypesIsSnapshot(t *testing.T) {
	r := queue.NewRegistry()
	r.Register("alpha", nopWorker)

	snap := r.TaskTypes()
	r.Register("beta", nopWorker) // add after snapshot

	if len(snap) != 1 {
		t.Errorf("snapshot len = %d, want 1 (should not reflect later changes)", len(snap))
	}
}

func TestRegistry_TaskTypesMatchesRegistered(t *testing.T) {
	r := queue.NewRegistry()
	registered := []string{"ingest", "export", "notify"}
	for _, tt := range registered {
		r.Register(tt, nopWorker)
	}

	got := r.TaskTypes()
	want := []string{"export", "ingest", "notify"} // sorted
	if !reflect.DeepEqual(got, want) {
		t.Errorf("TaskTypes = %v, want %v", got, want)
	}
}

// ── Concurrency smoke test ────────────────────────────────────────────────────

func TestRegistry_ConcurrentAccess(t *testing.T) {
	r := queue.NewRegistry()
	done := make(chan struct{})

	// Writer goroutine.
	go func() {
		for i := 0; i < 100; i++ {
			r.Register("job", nopWorker)
		}
		close(done)
	}()

	// Reader goroutines running concurrently with the writer.
	for i := 0; i < 4; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				r.Lookup("job")
				r.TaskTypes()
			}
		}()
	}

	<-done // test completes without data-race detector firing
}

// ── SetBridge / Dispatch / TaskTypes with bridge ──────────────────────────────

// shortSock returns a socket path inside a short-prefix temp dir so the full
// path stays well under the Unix 104-byte limit on macOS.
func shortSock(t *testing.T, name string) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "eq")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, name)
}

// startEchoServer starts a minimal Unix socket echo bridge server that returns
// the payload unchanged as a "result".
func startEchoServer(t *testing.T) string {
	t.Helper()
	sockPath := shortSock(t, "e.sock")
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
					resp, _ := json.Marshal(map[string]string{"result": "bridge-ok"})
					resp = append(resp, '\n')
					_, _ = c.Write(resp)
				}
			}(conn)
		}
	}()
	return sockPath
}

func makeTask(taskType string) *store.Task {
	return &store.Task{ID: "t1", Type: taskType, Payload: []byte(`{}`)}
}

func TestDispatch_GoType(t *testing.T) {
	r := queue.NewRegistry()
	var called bool
	r.Register("go_job", func(_ context.Context, _ []byte) ([]byte, error) {
		called = true
		return []byte(`"done"`), nil
	})

	result, err := r.Dispatch(context.Background(), makeTask("go_job"))
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if !called {
		t.Error("Go WorkerFunc was not called")
	}
	if string(result) != `"done"` {
		t.Errorf("result = %q, want \"done\"", result)
	}
}

func TestDispatch_PythonType(t *testing.T) {
	sockPath := startEchoServer(t)
	c := bridge.NewClient(sockPath)
	t.Cleanup(c.Close)

	r := queue.NewRegistry()
	r.SetBridge(c, []string{"py_job"})

	result, err := r.Dispatch(context.Background(), makeTask("py_job"))
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if string(result) != `"bridge-ok"` {
		t.Errorf("result = %q, want \"bridge-ok\"", result)
	}
}

func TestDispatch_UnknownType_ReturnsError(t *testing.T) {
	r := queue.NewRegistry()
	_, err := r.Dispatch(context.Background(), makeTask("mystery_type"))
	if err == nil {
		t.Fatal("expected error for unknown type, got nil")
	}
}

func TestDispatch_PanicRecovery(t *testing.T) {
	r := queue.NewRegistry()
	r.Register("panic_job", func(_ context.Context, _ []byte) ([]byte, error) {
		panic("test panic from worker")
	})

	result, err := r.Dispatch(context.Background(), makeTask("panic_job"))
	if err == nil {
		t.Fatal("expected error from panic recovery, got nil")
	}
	if result != nil {
		t.Errorf("result should be nil after panic, got %q", result)
	}
	if !errors.Is(err, err) { // basic non-nil check covered above
		t.Error("error should be non-nil")
	}
}

func TestDispatch_GoWorkerError(t *testing.T) {
	r := queue.NewRegistry()
	r.Register("failing_job", func(_ context.Context, _ []byte) ([]byte, error) {
		return nil, errors.New("intentional failure")
	})

	_, err := r.Dispatch(context.Background(), makeTask("failing_job"))
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != "intentional failure" {
		t.Errorf("err = %q, want \"intentional failure\"", err.Error())
	}
}

func TestTaskTypes_IncludesPythonAndGoTypes(t *testing.T) {
	sockPath := startEchoServer(t)
	c := bridge.NewClient(sockPath)
	t.Cleanup(c.Close)

	r := queue.NewRegistry()
	r.Register("go_type_a", nopWorker)
	r.Register("go_type_b", nopWorker)
	r.SetBridge(c, []string{"py_type_x", "py_type_y"})

	got := r.TaskTypes()
	want := []string{"go_type_a", "go_type_b", "py_type_x", "py_type_y"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("TaskTypes = %v, want %v", got, want)
	}
}

func TestTaskTypes_NoBridgeUnchanged(t *testing.T) {
	r := queue.NewRegistry()
	r.Register("only_go", nopWorker)
	// No SetBridge call.

	got := r.TaskTypes()
	if !reflect.DeepEqual(got, []string{"only_go"}) {
		t.Errorf("TaskTypes = %v, want [only_go]", got)
	}
}

func TestSetBridge_ReplacesOldConfig(t *testing.T) {
	sockPath := startEchoServer(t)
	c := bridge.NewClient(sockPath)
	t.Cleanup(c.Close)

	r := queue.NewRegistry()
	r.SetBridge(c, []string{"old_type"})
	r.SetBridge(c, []string{"new_type"}) // replaces

	types := r.TaskTypes()
	for _, tt := range types {
		if tt == "old_type" {
			t.Error("old_type should be removed after second SetBridge call")
		}
	}
	found := false
	for _, tt := range types {
		if tt == "new_type" {
			found = true
		}
	}
	if !found {
		t.Error("new_type should be present after second SetBridge call")
	}
}
