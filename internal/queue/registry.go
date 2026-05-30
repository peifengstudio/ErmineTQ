package queue

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/peifengstudio/erminetq/internal/bridge"
	"github.com/peifengstudio/erminetq/internal/store"
)

// WorkerFunc is the signature every task handler must satisfy.
// payload is the raw JSON bytes stored on the task; the return value becomes
// the attempt result (also stored as JSON).  A non-nil error marks the attempt
// as failed and triggers the retry/dead logic in the Store.
type WorkerFunc func(ctx context.Context, payload []byte) ([]byte, error)

// Registry maps task type strings to their handler functions and optionally
// routes Python task types to a Bridge client.
// It is safe for concurrent use.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]WorkerFunc

	bridgeClient *bridge.Client
	pythonTypes  map[string]struct{} // set of types routed to bridgeClient
}

// NewRegistry returns an empty Registry ready for use.
func NewRegistry() *Registry {
	return &Registry{
		handlers:    make(map[string]WorkerFunc),
		pythonTypes: make(map[string]struct{}),
	}
}

// Register associates taskType with fn.  If taskType was already registered
// the previous handler is silently replaced.  Panics if fn is nil.
func (r *Registry) Register(taskType string, fn WorkerFunc) {
	if fn == nil {
		panic("queue: Register called with nil WorkerFunc for type " + taskType)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[taskType] = fn
}

// SetBridge registers a Bridge client and declares which task types should be
// routed to it.  After this call TaskTypes() includes the Python types and
// Dispatch routes them to client.Call rather than a WorkerFunc.
// Pass nil client to disable bridge routing.
func (r *Registry) SetBridge(client *bridge.Client, taskTypes []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.bridgeClient = client
	r.pythonTypes = make(map[string]struct{}, len(taskTypes))
	for _, t := range taskTypes {
		r.pythonTypes[t] = struct{}{}
	}
}

// Lookup returns the WorkerFunc registered for taskType and true, or nil and
// false when taskType has no Go handler.
func (r *Registry) Lookup(taskType string) (WorkerFunc, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	fn, ok := r.handlers[taskType]
	return fn, ok
}

// Dispatch is the unified execution entry point used by RunOnce.
//
//   - Go type  (registered via Register)  → WorkerFunc is called
//   - Python type (registered via SetBridge) → Bridge.Call is invoked
//   - Unknown type                          → error (should not happen in
//     normal operation since ClaimTask only returns known types)
//
// Panics inside WorkerFuncs are caught and returned as errors.
func (r *Registry) Dispatch(ctx context.Context, task *store.Task) (result []byte, err error) {
	// Panic recovery protects both Go and Python dispatch paths.
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("panic: %v", rec)
			result = nil
		}
	}()

	r.mu.RLock()
	fn, isGo := r.handlers[task.Type]
	_, isPython := r.pythonTypes[task.Type]
	client := r.bridgeClient
	r.mu.RUnlock()

	switch {
	case isGo:
		return fn(ctx, task.Payload)
	case isPython && client != nil:
		return client.Call(ctx, task.ID, task.Type, task.Payload)
	default:
		return nil, fmt.Errorf("queue: no handler registered for task type %q", task.Type)
	}
}

// TaskTypes returns a sorted slice of all registered task type strings,
// including both Go handler types and Python Bridge types.
// The slice is a snapshot — modifications to the Registry afterwards are not
// reflected.  The sorted order gives ClaimTask a deterministic type list.
func (r *Registry) TaskTypes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	seen := make(map[string]struct{}, len(r.handlers)+len(r.pythonTypes))
	for t := range r.handlers {
		seen[t] = struct{}{}
	}
	for t := range r.pythonTypes {
		seen[t] = struct{}{}
	}

	types := make([]string, 0, len(seen))
	for t := range seen {
		types = append(types, t)
	}
	sort.Strings(types)
	return types
}
