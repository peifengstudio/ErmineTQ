package scheduler

import (
	"context"
	"sync"
	"time"
)

// heartbeatScanner wakes up every ScanInterval, finds running attempts whose
// last heartbeat is older than HeartbeatCutoff, and marks each one as timed
// out so the task can retry or go dead.
type heartbeatScanner struct {
	store    SchedulerStore
	interval time.Duration
	cutoff   time.Duration // age threshold: attempts older than this are stale
	onError  func(error)
	wg       sync.WaitGroup
}

func newHeartbeatScanner(s SchedulerStore, interval, cutoff time.Duration, onError func(error)) *heartbeatScanner {
	return &heartbeatScanner{store: s, interval: interval, cutoff: cutoff, onError: onError}
}

func (h *heartbeatScanner) start(ctx context.Context) {
	h.wg.Add(1)
	go h.run(ctx)
}

func (h *heartbeatScanner) wait() { h.wg.Wait() }

func (h *heartbeatScanner) run(ctx context.Context) {
	defer h.wg.Done()

	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.scan(ctx)
		}
	}
}

// scan performs one sweep: find stale attempts and time out each one.
// Errors for individual attempts are reported via onError but do not abort the
// sweep — we want to time out as many stale attempts as possible per cycle.
func (h *heartbeatScanner) scan(ctx context.Context) {
	cutoff := time.Now().UTC().Add(-h.cutoff)

	attempts, err := h.store.FindStaleAttempts(ctx, cutoff)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		if h.onError != nil {
			h.onError(err)
		}
		return
	}

	for _, a := range attempts {
		if ctx.Err() != nil {
			return
		}
		if err := h.store.TimeoutAttempt(ctx, a.ID); err != nil {
			if ctx.Err() != nil {
				return
			}
			if h.onError != nil {
				h.onError(err)
			}
		}
	}
}
