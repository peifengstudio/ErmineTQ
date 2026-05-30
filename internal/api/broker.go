package api

import (
	"context"
	"sync"

	"github.com/peifengstudio/erminetq/internal/store"
)

// Compile-time guarantee: *Broker implements store.EventSink.
var _ store.EventSink = (*Broker)(nil)

const (
	// brokerInputBuf is the number of events buffered in the Broker's
	// internal fan-out channel.  Events are dropped when it fills up.
	brokerInputBuf = 256

	// subBufSize is the per-subscriber channel buffer.  A subscriber that
	// consumes faster than this size events per fan-out cycle will not lose
	// any events; slower subscribers have events silently dropped.
	subBufSize = 64
)

// Broker is an in-process SSE event bus.  It receives events via Emit and
// fans them out to every active subscriber.
//
// Slow subscribers do not block fast ones: if a subscriber's buffer is full
// the event is dropped for that subscriber only.
type Broker struct {
	// input carries events from Emit to the fan-out goroutine.
	input chan store.SSEEvent

	// subs maps the read-only channel returned to callers (key) to the
	// internal bidirectional channel used for writes (value).
	subsMu sync.Mutex
	subs   map[<-chan store.SSEEvent]chan store.SSEEvent

	wg sync.WaitGroup
}

// NewBroker creates a Broker ready to Start.
func NewBroker() *Broker {
	return &Broker{
		input: make(chan store.SSEEvent, brokerInputBuf),
		subs:  make(map[<-chan store.SSEEvent]chan store.SSEEvent),
	}
}

// Start launches the fan-out goroutine.  Cancel ctx to stop it, then call
// Wait to block until it exits.
func (b *Broker) Start(ctx context.Context) {
	b.wg.Add(1)
	go b.run(ctx)
}

// Wait blocks until the goroutine started by Start has returned.
func (b *Broker) Wait() {
	b.wg.Wait()
}

// Emit implements store.EventSink.  It is non-blocking: the event is dropped
// if the internal input channel is full.
func (b *Broker) Emit(ev store.SSEEvent) {
	select {
	case b.input <- ev:
	default:
		// input buffer full: drop
	}
}

// Subscribe registers a new subscriber and returns a receive-only channel on
// which events will be delivered.  The caller must eventually call Unsubscribe
// to avoid leaking the channel.
func (b *Broker) Subscribe() <-chan store.SSEEvent {
	ch := make(chan store.SSEEvent, subBufSize)
	b.subsMu.Lock()
	b.subs[ch] = ch // chan T is assignable to <-chan T used as the key
	b.subsMu.Unlock()
	return ch
}

// Unsubscribe removes the subscriber identified by ch.  No further events
// will be delivered to ch after this call returns.
func (b *Broker) Unsubscribe(ch <-chan store.SSEEvent) {
	b.subsMu.Lock()
	delete(b.subs, ch)
	b.subsMu.Unlock()
}

// run is the single fan-out goroutine.
func (b *Broker) run(ctx context.Context) {
	defer b.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-b.input:
			b.fanOut(ev)
		}
	}
}

// fanOut delivers ev to every current subscriber.
// Subscribers whose buffer is full receive a dropped event (non-blocking select).
func (b *Broker) fanOut(ev store.SSEEvent) {
	b.subsMu.Lock()
	defer b.subsMu.Unlock()
	for _, bidi := range b.subs {
		select {
		case bidi <- ev:
		default:
			// slow subscriber: drop
		}
	}
}
