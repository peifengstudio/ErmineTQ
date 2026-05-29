package queue

import (
	"context"
	"sort"
	"sync"
)

// WorkerFunc is the signature every task handler must satisfy.
// payload is the raw JSON bytes stored on the task; the return value becomes
// the attempt result (also stored as JSON).  A non-nil error marks the attempt
// as failed and triggers the retry/dead logic in the Store.
type WorkerFunc func(ctx context.Context, payload []byte) ([]byte, error)

// Registry maps task type strings to their handler functions.
// It is safe for concurrent use: Register may be called from multiple
// goroutines during startup, and Lookup is called from the worker pool.
type Registry struct {
	mu      sync.RWMutex
	handlers map[string]WorkerFunc
}

// NewRegistry returns an empty Registry ready for use.
func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]WorkerFunc)}
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

// Lookup returns the WorkerFunc registered for taskType and true, or nil and
// false when taskType has no handler.
func (r *Registry) Lookup(taskType string) (WorkerFunc, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	fn, ok := r.handlers[taskType]
	return fn, ok
}

// TaskTypes returns a sorted slice of all registered task type strings.
// The slice is a snapshot — modifications to the Registry afterwards are not
// reflected.  The sorted order gives ClaimTask a deterministic type list.
func (r *Registry) TaskTypes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	types := make([]string, 0, len(r.handlers))
	for t := range r.handlers {
		types = append(types, t)
	}
	sort.Strings(types)
	return types
}
