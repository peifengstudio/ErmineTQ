package api_test

import (
	"context"
	"testing"
	"time"

	"github.com/peifengstudio/erminetq/internal/api"
	"github.com/peifengstudio/erminetq/internal/store"
)

// makeEvent returns a minimal SSEEvent with a recognisable task ID.
func makeEvent(taskID string) store.SSEEvent {
	return store.SSEEvent{
		TaskID:    taskID,
		Event:     store.TaskEventQueued,
		Timestamp: time.Now().UTC(),
	}
}

// startBroker starts a Broker and registers cleanup so that cancel() is
// always called before Wait() regardless of test outcome.
func startBroker(t *testing.T) (*api.Broker, context.CancelFunc) {
	t.Helper()
	b := api.NewBroker()
	ctx, cancel := context.WithCancel(context.Background())
	b.Start(ctx)
	t.Cleanup(func() { cancel(); b.Wait() })
	return b, cancel
}

// ── tests ──────────────────────────────────────────────────────────────────────

func TestBroker_FanOut_MultipleSubscribers(t *testing.T) {
	b, _ := startBroker(t)

	sub1 := b.Subscribe()
	sub2 := b.Subscribe()
	t.Cleanup(func() { b.Unsubscribe(sub1); b.Unsubscribe(sub2) })

	ev := makeEvent("task-fanout")
	b.Emit(ev)

	timeout := time.After(500 * time.Millisecond)
	for _, ch := range []<-chan store.SSEEvent{sub1, sub2} {
		select {
		case got := <-ch:
			if got.TaskID != ev.TaskID {
				t.Errorf("received task_id %q, want %q", got.TaskID, ev.TaskID)
			}
		case <-timeout:
			t.Fatal("timed out waiting for event on a subscriber channel")
		}
	}
}

func TestBroker_SlowSubscriber_DoesNotBlockOthers(t *testing.T) {
	b, _ := startBroker(t)

	// fast subscriber — we will drain it
	fast := b.Subscribe()
	t.Cleanup(func() { b.Unsubscribe(fast) })

	// slow subscriber — never drained, so its buffer fills up
	slow := b.Subscribe()
	t.Cleanup(func() { b.Unsubscribe(slow) })

	// Fill the slow sub buffer with one event, let the fan-out deliver.
	b.Emit(makeEvent("fill-slow"))
	time.Sleep(20 * time.Millisecond)
	// Drain fast to get back to a clean state.
	select {
	case <-fast:
	default:
	}

	// Emit more events. The slow sub is at capacity; its events should be dropped.
	for i := 0; i < 3; i++ {
		b.Emit(makeEvent("after-fill"))
	}

	// fast must receive all three events without blocking.
	received := 0
	deadline := time.After(500 * time.Millisecond)
	for received < 3 {
		select {
		case <-fast:
			received++
		case <-deadline:
			t.Fatalf("fast subscriber only got %d/3 events; slow subscriber blocked fan-out", received)
		}
	}
}

func TestBroker_Unsubscribe_StopsDelivery(t *testing.T) {
	b, _ := startBroker(t)

	ch := b.Subscribe()
	b.Unsubscribe(ch) // unsubscribe immediately

	b.Emit(makeEvent("after-unsub"))
	time.Sleep(30 * time.Millisecond)

	select {
	case ev := <-ch:
		t.Errorf("received event %v after unsubscribe", ev)
	default:
		// correct: no events
	}
}

func TestBroker_StartWait_CleanShutdown(t *testing.T) {
	b := api.NewBroker()
	ctx, cancel := context.WithCancel(context.Background())
	b.Start(ctx)
	cancel()

	done := make(chan struct{})
	go func() { b.Wait(); close(done) }()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Broker.Wait() did not return after ctx cancellation")
	}
}

func TestBroker_EmitNonBlocking_WhenInputFull(t *testing.T) {
	// Do NOT start the broker — input is never drained so it fills quickly.
	b := api.NewBroker()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 512; i++ {
			b.Emit(makeEvent("flood"))
		}
	}()

	select {
	case <-done:
		// All Emit calls returned without blocking.
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Emit blocked when input channel was full")
	}
}

func TestBroker_MultipleSubscribersIndependent(t *testing.T) {
	b, _ := startBroker(t)

	const n = 4
	subs := make([]<-chan store.SSEEvent, n)
	for i := range subs {
		subs[i] = b.Subscribe()
		t.Cleanup(func() { b.Unsubscribe(subs[i]) })
	}

	b.Emit(makeEvent("multi"))

	deadline := time.After(500 * time.Millisecond)
	for i, ch := range subs {
		select {
		case got := <-ch:
			if got.TaskID != "multi" {
				t.Errorf("sub[%d] got task_id %q, want %q", i, got.TaskID, "multi")
			}
		case <-deadline:
			t.Fatalf("sub[%d] did not receive event", i)
		}
	}
}

func TestBroker_EventFieldsPreserved(t *testing.T) {
	b, _ := startBroker(t)

	ch := b.Subscribe()
	t.Cleanup(func() { b.Unsubscribe(ch) })

	sent := store.SSEEvent{
		TaskID:    "task-xyz",
		Event:     store.TaskEventSucceeded,
		Timestamp: time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC),
	}
	b.Emit(sent)

	select {
	case got := <-ch:
		if got.TaskID != sent.TaskID {
			t.Errorf("TaskID = %q, want %q", got.TaskID, sent.TaskID)
		}
		if got.Event != sent.Event {
			t.Errorf("Event = %q, want %q", got.Event, sent.Event)
		}
		if !got.Timestamp.Equal(sent.Timestamp) {
			t.Errorf("Timestamp = %v, want %v", got.Timestamp, sent.Timestamp)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("event not received")
	}
}
