package api

import (
	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/peifengstudio/erminetq/internal/store"
)

// startBroker starts the broker and registers cleanup in the right order:
// cancel first, then Wait, so the goroutine exits cleanly.
func startBroker(t *testing.T) *Broker {
	t.Helper()
	broker := NewBroker()
	ctx, cancel := context.WithCancel(context.Background())
	broker.Start(ctx)
	t.Cleanup(func() {
		cancel()
		broker.Wait()
	})
	return broker
}

func TestHandleEvents_ReceivesEvent(t *testing.T) {
	broker := startBroker(t)
	handler := NewHandler(&mockStore{}, broker)

	srv := httptest.NewServer(http.HandlerFunc(handler.HandleEvents))
	t.Cleanup(srv.Close)

	resp, err := srv.Client().Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })

	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("Content-Type = %q, want text/event-stream", ct)
	}

	// Give the handler time to register the subscriber.
	time.Sleep(20 * time.Millisecond)

	broker.Emit(store.SSEEvent{
		TaskID:    "t42",
		Event:     store.TaskEventQueued,
		Timestamp: time.Now().UTC(),
	})

	done := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "data:") {
				done <- line
				return
			}
		}
	}()

	select {
	case line := <-done:
		if !strings.Contains(line, "t42") {
			t.Errorf("expected task_id t42 in SSE line, got: %s", line)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for SSE event")
	}
}

func TestHandleEvents_UnsubscribeOnDisconnect(t *testing.T) {
	broker := startBroker(t)
	handler := NewHandler(&mockStore{}, broker)

	srv := httptest.NewServer(http.HandlerFunc(handler.HandleEvents))
	t.Cleanup(srv.Close)

	resp, err := srv.Client().Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(20 * time.Millisecond)

	broker.subsMu.Lock()
	countBefore := len(broker.subs)
	broker.subsMu.Unlock()

	if countBefore == 0 {
		t.Fatal("expected at least one subscriber after connecting")
	}

	// Close the connection — handler should call Unsubscribe.
	resp.Body.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		broker.subsMu.Lock()
		n := len(broker.subs)
		broker.subsMu.Unlock()
		if n == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Error("subscriber was not removed after connection closed")
}
