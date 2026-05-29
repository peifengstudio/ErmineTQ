package scheduler

import (
	"context"
	"sync"
	"time"
)

// retryScheduler wakes up every RetryInterval and promotes retrying tasks
// whose run_at has elapsed back to queued so workers can pick them up again.
type retryScheduler struct {
	store    SchedulerStore
	interval time.Duration
	onError  func(error)
	wg       sync.WaitGroup
}

func newRetryScheduler(s SchedulerStore, interval time.Duration, onError func(error)) *retryScheduler {
	return &retryScheduler{store: s, interval: interval, onError: onError}
}

func (r *retryScheduler) start(ctx context.Context) {
	r.wg.Add(1)
	go r.run(ctx)
}

func (r *retryScheduler) wait() { r.wg.Wait() }

func (r *retryScheduler) run(ctx context.Context) {
	defer r.wg.Done()

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := r.store.RequeueRetrying(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return // normal shutdown, not an error
				}
				if r.onError != nil {
					r.onError(err)
				}
				continue
			}
			_ = n // Phase 4: log.Printf("requeued %d retrying tasks", n)
		}
	}
}
