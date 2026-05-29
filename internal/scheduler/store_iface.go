package scheduler

import (
	"context"
	"time"

	"github.com/peifengstudio/erminetq/internal/store"
)

// SchedulerStore is the subset of *store.Store the scheduler needs.
// Keeping the interface narrow makes every scheduler component independently
// testable with a simple struct mock.
//
// The compile-time assertion at the bottom guarantees *store.Store stays in sync.
type SchedulerStore interface {
	// ── Maintenance (Phase 3.1) ───────────────────────────────────────────────

	// RequeueRetrying promotes all retrying tasks whose run_at has passed back
	// to queued status.  Returns the number of tasks transitioned.
	RequeueRetrying(ctx context.Context) (int, error)

	// FindStaleAttempts returns running attempts whose last heartbeat (or
	// started_at) is at or before cutoff.
	FindStaleAttempts(ctx context.Context, cutoff time.Time) ([]*store.Attempt, error)

	// TimeoutAttempt fails a stale attempt and moves the task to retrying or
	// dead, emitting a heartbeat_timeout event.
	TimeoutAttempt(ctx context.Context, attemptID string) error

	// ── Task scheduling (Phase 3.2) ───────────────────────────────────────────

	// DueSchedules returns enabled schedules whose next_run_at is in the past.
	DueSchedules(ctx context.Context) ([]*store.Schedule, error)

	// RecordScheduleRun advances a schedule's last_run_at and next_run_at.
	RecordScheduleRun(ctx context.Context, id string, nextRunAt time.Time) error

	// CreateTask inserts a new queued task row.
	CreateTask(ctx context.Context, in store.CreateTaskInput) (*store.Task, error)

	// ListSchedules returns all schedule rows (used for initial cron wiring).
	ListSchedules(ctx context.Context) ([]*store.Schedule, error)

	// UpdateSchedule patches a schedule row (used to enable/disable schedules).
	UpdateSchedule(ctx context.Context, id string, in store.UpdateScheduleInput) (*store.Schedule, error)
}

// Compile-time guarantee: *store.Store must satisfy SchedulerStore.
var _ SchedulerStore = (*store.Store)(nil)
