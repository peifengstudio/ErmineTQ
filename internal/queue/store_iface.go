package queue

import (
	"context"
	"encoding/json"

	"github.com/peifengstudio/erminetq/internal/config"
	"github.com/peifengstudio/erminetq/internal/store"
)

// TaskStore is the subset of *store.Store that the worker pool needs.
// Keeping the interface narrow makes each component testable with a simple
// struct mock instead of a full SQLite database.
//
// The compile-time assertion below guarantees *store.Store stays in sync.
type TaskStore interface {
	// CreateWorker registers this pool as a worker in the store.
	// Called once by NewPool; the returned ID is used for all subsequent
	// ClaimTask and attempt-outcome calls.
	CreateWorker(ctx context.Context, in store.CreateWorkerInput) (*store.Worker, error)

	// ClaimTask atomically acquires one queued task and returns it together
	// with the ID of the newly created attempt row.  Returns (nil, "", nil)
	// when the queue is empty or all limits are reached.
	ClaimTask(
		ctx context.Context,
		workerID, queue string,
		taskTypes []string,
		cfg *config.Config,
	) (*store.Task, string, error)

	// SucceedAttempt records a successful execution result.
	SucceedAttempt(ctx context.Context, attemptID string, result json.RawMessage) error

	// FailAttempt records an execution failure; the store decides whether to
	// transition the task to retrying or dead based on the retry budget.
	FailAttempt(ctx context.Context, attemptID, errMsg string) error

	// CancelAttempt records a cooperative cancellation (e.g. halt signal).
	CancelAttempt(ctx context.Context, attemptID string) error

	// UpdateHeartbeat marks the attempt as still alive.  Called periodically
	// by the heartbeat goroutine inside RunOnce.
	UpdateHeartbeat(ctx context.Context, attemptID string) error
}

// Compile-time guarantee: *store.Store must satisfy TaskStore.
// If this line fails to compile, a method was added to the interface but not
// yet implemented on *store.Store (or vice-versa).
var _ TaskStore = (*store.Store)(nil)
