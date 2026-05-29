package scheduler

import (
	"context"
	"sync"
	"time"

	"github.com/peifengstudio/erminetq/internal/store"
)

// taskScheduler polls for due schedules and creates a task for each one,
// then advances the schedule's next_run_at.
type taskScheduler struct {
	store    SchedulerStore
	interval time.Duration // TaskPollInterval
	onError  func(error)
	wg       sync.WaitGroup
}

func newTaskScheduler(s SchedulerStore, interval time.Duration, onError func(error)) *taskScheduler {
	return &taskScheduler{store: s, interval: interval, onError: onError}
}

func (t *taskScheduler) start(ctx context.Context) {
	t.wg.Add(1)
	go t.run(ctx)
}

func (t *taskScheduler) wait() { t.wg.Wait() }

func (t *taskScheduler) run(ctx context.Context) {
	defer t.wg.Done()

	// On startup: set next_run_at for any enabled schedule that has none.
	// This prevents tasks from being skipped on restart.
	t.initSchedules(ctx)
	if ctx.Err() != nil {
		return
	}

	ticker := time.NewTicker(t.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t.poll(ctx)
		}
	}
}

// initSchedules writes next_run_at for every enabled schedule whose
// next_run_at is currently nil.
func (t *taskScheduler) initSchedules(ctx context.Context) {
	schedules, err := t.store.ListSchedules(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		if t.onError != nil {
			t.onError(err)
		}
		return
	}

	now := time.Now().UTC()
	for _, sc := range schedules {
		if !sc.Enabled || sc.NextRunAt != nil {
			continue // skip disabled or already-initialised schedules
		}
		next, err := computeNextRun(sc, now)
		if err != nil {
			if t.onError != nil {
				t.onError(err)
			}
			continue
		}
		if _, err := t.store.UpdateSchedule(ctx, sc.ID, store.UpdateScheduleInput{
			NextRunAt: &next,
		}); err != nil {
			if ctx.Err() != nil {
				return
			}
			if t.onError != nil {
				t.onError(err)
			}
		}
	}
}

// poll fetches all due schedules and fires a task for each one.
func (t *taskScheduler) poll(ctx context.Context) {
	due, err := t.store.DueSchedules(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		if t.onError != nil {
			t.onError(err)
		}
		return
	}

	now := time.Now().UTC()
	for _, sc := range due {
		if ctx.Err() != nil {
			return
		}
		t.fireDue(ctx, sc, now)
	}
}

// fireDue creates one task for a due schedule and advances next_run_at.
// A CreateTask failure aborts the run for this schedule (no record update).
// A computeNextRun failure after task creation is reported but does not
// roll back the already-created task.
func (t *taskScheduler) fireDue(ctx context.Context, sc *store.Schedule, now time.Time) {
	_, err := t.store.CreateTask(ctx, store.CreateTaskInput{
		Type:       sc.TaskType,
		Queue:      sc.Queue,
		Payload:    sc.Payload,
		ScheduleID: &sc.ID,
	})
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		if t.onError != nil {
			t.onError(err)
		}
		return
	}

	next, err := computeNextRun(sc, now)
	if err != nil {
		// The task was created but we can't advance next_run_at.
		// The schedule will fire again on the next poll tick (at-least-once).
		if t.onError != nil {
			t.onError(err)
		}
		return
	}

	if err := t.store.RecordScheduleRun(ctx, sc.ID, next); err != nil {
		if ctx.Err() != nil {
			return
		}
		if t.onError != nil {
			t.onError(err)
		}
	}
}
