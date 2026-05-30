package api

import (
	"context"

	"github.com/peifengstudio/erminetq/internal/store"
)

// APIStore is the subset of store.Store that HTTP handlers depend on.
// The full store.Store satisfies this interface (enforced below).
type APIStore interface {
	// Tasks
	CreateTask(ctx context.Context, in store.CreateTaskInput) (*store.Task, error)
	GetTask(ctx context.Context, id string) (*store.Task, []*store.Attempt, []*store.TaskEvent, error)
	ListTasks(ctx context.Context, f store.ListTasksFilter) ([]*store.Task, error)
	HaltTask(ctx context.Context, id string) error
	ResumeTask(ctx context.Context, id string) error
	CancelTask(ctx context.Context, id string) error
	RetryTask(ctx context.Context, id string) error
	RestartTask(ctx context.Context, id string) (*store.Task, error)
	// Workers
	CreateWorker(ctx context.Context, in store.CreateWorkerInput) (*store.Worker, error)
	ListWorkers(ctx context.Context) ([]*store.Worker, error)
	// Schedules
	CreateSchedule(ctx context.Context, in store.CreateScheduleInput) (*store.Schedule, error)
	GetSchedule(ctx context.Context, id string) (*store.Schedule, error)
	ListSchedules(ctx context.Context) ([]*store.Schedule, error)
	UpdateSchedule(ctx context.Context, id string, in store.UpdateScheduleInput) (*store.Schedule, error)
}

var _ APIStore = (*store.Store)(nil)
