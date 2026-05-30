package bridge_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/peifengstudio/erminetq/internal/bridge"
)

// shortSock creates a short-path temp directory and returns a socket path
// inside it.  Unix socket paths are limited to ~104 bytes on macOS; t.TempDir()
// embeds the full test name and can exceed that limit.
func shortSock(t *testing.T, name string) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "eq")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, name)
}

// ── mock server helpers ───────────────────────────────────────────────────────

// mockServer is a minimal Unix socket server for testing.
type mockServer struct {
	ln      net.Listener
	wg      sync.WaitGroup
	handler func(req map[string]json.RawMessage) (resp []byte)
}

func newMockServer(t *testing.T, handler func(map[string]json.RawMessage) []byte) *mockServer {
	t.Helper()
	sockPath := shortSock(t, "s.sock")
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ms := &mockServer{ln: ln, handler: handler}
	ms.wg.Add(1)
	go ms.serve()
	t.Cleanup(func() {
		_ = ln.Close()
		ms.wg.Wait()
	})
	return ms
}

func (ms *mockServer) path() string { return ms.ln.Addr().String() }

func (ms *mockServer) serve() {
	defer ms.wg.Done()
	for {
		conn, err := ms.ln.Accept()
		if err != nil {
			return // listener closed
		}
		go ms.handleConn(conn)
	}
}

func (ms *mockServer) handleConn(conn net.Conn) {
	defer conn.Close()
	sc := bufio.NewScanner(conn)
	for sc.Scan() {
		var raw map[string]json.RawMessage
		if err := json.Unmarshal(sc.Bytes(), &raw); err != nil {
			return
		}
		resp := ms.handler(raw)
		resp = append(resp, '\n')
		_, _ = conn.Write(resp)
	}
}

// jsonResp builds a success response.
func jsonResp(result any) []byte {
	type successResp struct {
		Result any `json:"result"`
	}
	b, _ := json.Marshal(successResp{Result: result})
	return b
}

// jsonErr builds an error response.
func jsonErr(msg string) []byte {
	type errResp struct {
		Error string `json:"error"`
	}
	b, _ := json.Marshal(errResp{Error: msg})
	return b
}

// ── tests ─────────────────────────────────────────────────────────────────────

func TestCall_Success(t *testing.T) {
	ms := newMockServer(t, func(req map[string]json.RawMessage) []byte {
		return jsonResp(map[string]string{"echo": "ok"})
	})

	c := bridge.NewClient(ms.path())
	defer c.Close()

	result, err := c.Call(context.Background(), "t1", "analyze_data", []byte(`{"x":1}`))
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	var got map[string]string
	if err := json.Unmarshal(result, &got); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	if got["echo"] != "ok" {
		t.Errorf("result = %v", got)
	}
}

func TestCall_PythonReturnsError(t *testing.T) {
	ms := newMockServer(t, func(_ map[string]json.RawMessage) []byte {
		return jsonErr("something went wrong in python")
	})

	c := bridge.NewClient(ms.path())
	defer c.Close()

	_, err := c.Call(context.Background(), "t1", "analyze_data", nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != "something went wrong in python" {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestCall_SocketNotExist(t *testing.T) {
	c := bridge.NewClient("/nonexistent/bridge.sock")
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := c.Call(ctx, "t1", "analyze_data", nil)
	if err == nil {
		t.Fatal("expected error for nonexistent socket, got nil")
	}
}

func TestCall_CtxTimeout(t *testing.T) {
	// Server that accepts connections but never writes back.
	sockPath := shortSock(t, "slow.sock")
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 1024)
				for {
					if _, err := c.Read(buf); err != nil {
						return
					}
				}
			}(conn)
		}
	}()

	c := bridge.NewClient(sockPath)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	_, err = c.Call(ctx, "t1", "analyze_data", nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected DeadlineExceeded, got: %v", err)
	}
}

func TestCall_CtxCancelled(t *testing.T) {
	sockPath := shortSock(t, "cancel.sock")
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 1024)
				for {
					if _, err := c.Read(buf); err != nil {
						return
					}
				}
			}(conn)
		}
	}()

	c := bridge.NewClient(sockPath)
	defer c.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	_, err = c.Call(ctx, "t1", "analyze_data", nil)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected Canceled, got: %v", err)
	}
}

func TestCancel_NoConnNoPanic(t *testing.T) {
	c := bridge.NewClient("/nonexistent/bridge.sock")
	c.Cancel("t1") // must not panic
}

func TestCancel_AfterClose(t *testing.T) {
	ms := newMockServer(t, func(_ map[string]json.RawMessage) []byte {
		return jsonResp(nil)
	})

	c := bridge.NewClient(ms.path())
	if _, err := c.Call(context.Background(), "t1", "job", nil); err != nil {
		t.Fatalf("Call: %v", err)
	}
	c.Close()
	c.Cancel("t1") // must not panic after close
}

func TestCall_AfterClose(t *testing.T) {
	ms := newMockServer(t, func(_ map[string]json.RawMessage) []byte {
		return jsonResp(nil)
	})

	c := bridge.NewClient(ms.path())
	c.Close()

	_, err := c.Call(context.Background(), "t1", "job", nil)
	if err == nil {
		t.Fatal("expected error after Close, got nil")
	}
}

func TestCall_RequestFields(t *testing.T) {
	var gotTaskID, gotType string
	var gotPayload json.RawMessage

	ms := newMockServer(t, func(req map[string]json.RawMessage) []byte {
		_ = json.Unmarshal(req["task_id"], &gotTaskID)
		_ = json.Unmarshal(req["type"], &gotType)
		gotPayload = req["payload"]
		return jsonResp("done")
	})

	c := bridge.NewClient(ms.path())
	defer c.Close()

	payload := []byte(`{"key":"value"}`)
	if _, err := c.Call(context.Background(), "task-abc", "my_type", payload); err != nil {
		t.Fatalf("Call: %v", err)
	}
	if gotTaskID != "task-abc" {
		t.Errorf("task_id = %q, want task-abc", gotTaskID)
	}
	if gotType != "my_type" {
		t.Errorf("type = %q, want my_type", gotType)
	}
	if string(gotPayload) != `{"key":"value"}` {
		t.Errorf("payload = %s, want {\"key\":\"value\"}", gotPayload)
	}
}

func TestCall_ReconnectAfterServerRestart(t *testing.T) {
	sockPath := shortSock(t, "r.sock")

	// First server instance.
	ln1, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("listen1: %v", err)
	}
	serve := func(ln net.Listener) {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				sc := bufio.NewScanner(c)
				for sc.Scan() {
					resp := append(jsonResp("pong"), '\n')
					_, _ = c.Write(resp)
				}
			}(conn)
		}
	}
	go serve(ln1)

	c := bridge.NewClient(sockPath)
	defer c.Close()

	if _, err := c.Call(context.Background(), "t1", "ping", nil); err != nil {
		t.Fatalf("first call: %v", err)
	}

	// Simulate server restart.
	_ = ln1.Close()
	_ = os.Remove(sockPath)
	time.Sleep(20 * time.Millisecond)

	ln2, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("listen2: %v", err)
	}
	defer ln2.Close()
	go serve(ln2)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := c.Call(ctx, "t2", "ping", nil); err != nil {
		t.Fatalf("call after restart: %v", err)
	}
}
