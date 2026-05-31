package queue

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/peifengstudio/erminetq/internal/store"
)

// WorkerFunc is the signature every Go task handler must satisfy.
// payload is the raw JSON bytes stored on the task; the return value becomes
// the attempt result (also stored as JSON).  A non-nil error marks the attempt
// as failed and triggers the retry/dead logic in the Store.
type WorkerFunc func(ctx context.Context, payload []byte) ([]byte, error)

// Registry maps task type strings to their Go handler functions.
// Python task types are handled by the Python SDK pull workers via HTTP —
// they do not appear in this registry.
// It is safe for concurrent use.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]WorkerFunc
}

// NewRegistry returns an empty Registry ready for use.
func NewRegistry() *Registry {
	return &Registry{
		handlers: make(map[string]WorkerFunc),
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

// Lookup returns the WorkerFunc registered for taskType and true, or nil and
// false when taskType has no Go handler.
func (r *Registry) Lookup(taskType string) (WorkerFunc, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	fn, ok := r.handlers[taskType]
	return fn, ok
}

// Dispatch executes the handler registered for task.Type.
// Returns an error when no handler is registered or a handler panics.
//
// Panics inside WorkerFuncs are caught and returned as errors.
func (r *Registry) Dispatch(ctx context.Context, task *store.Task) (result []byte, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("panic: %v", rec)
			result = nil
		}
	}()

	r.mu.RLock()
	fn, ok := r.handlers[task.Type]
	r.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("queue: no handler registered for task type %q", task.Type)
	}
	return fn(ctx, task.Payload)
}

// TaskTypes returns a sorted snapshot of all registered Go task type strings.
// The sorted order gives ClaimTask a deterministic type list.
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
