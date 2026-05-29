package scheduler

import (
	"context"
	"time"
)

// Config holds all tunable parameters for the Scheduler.
// Zero values trigger safe defaults.
type Config struct {
	// RetryInterval is how often retrying tasks are promoted back to queued.
	// Default: 5 s.
	RetryInterval time.Duration

	// ScanInterval is how often the heartbeat scanner runs.
	// Default: 30 s.
	ScanInterval time.Duration

	// HeartbeatCutoff is the age threshold for stale attempts: a running
	// attempt whose last heartbeat (or started_at) is older than this value
	// is timed out.  Default: 60 s.
	HeartbeatCutoff time.Duration

	// TaskPollInterval controls how often the task scheduler checks for due
	// schedules.  Reserved for Phase 3.2.  Default: 10 s.
	TaskPollInterval time.Duration

	// OnError is called for non-fatal store errors encountered by any
	// sub-scheduler.  Nil means errors are silently discarded.
	OnError func(error)
}

func (c *Config) applyDefaults() {
	if c.RetryInterval <= 0 {
		c.RetryInterval = 5 * time.Second
	}
	if c.ScanInterval <= 0 {
		c.ScanInterval = 30 * time.Second
	}
	if c.HeartbeatCutoff <= 0 {
		c.HeartbeatCutoff = 60 * time.Second
	}
	if c.TaskPollInterval <= 0 {
		c.TaskPollInterval = 10 * time.Second
	}
}

// Scheduler owns and coordinates all background maintenance goroutines:
//   - retryScheduler  — promotes retrying tasks back to queued
//   - heartbeatScanner — times out stale running attempts
//   - taskScheduler   — fires cron/interval schedule tasks
type Scheduler struct {
	retry   *retryScheduler
	scanner *heartbeatScanner
	task    *taskScheduler
}

// New constructs a Scheduler with the provided store and configuration.
// Call Start to begin background processing.
func New(s SchedulerStore, cfg Config) *Scheduler {
	cfg.applyDefaults()
	return &Scheduler{
		retry:   newRetryScheduler(s, cfg.RetryInterval, cfg.OnError),
		scanner: newHeartbeatScanner(s, cfg.ScanInterval, cfg.HeartbeatCutoff, cfg.OnError),
		task:    newTaskScheduler(s, cfg.TaskPollInterval, cfg.OnError),
	}
}

// Start launches all background goroutines.  It is non-blocking; pair it
// with Wait to drain when ctx is cancelled.
func (s *Scheduler) Start(ctx context.Context) {
	s.retry.start(ctx)
	s.scanner.start(ctx)
	s.task.start(ctx)
}

// Wait blocks until all goroutines started by Start have returned.
func (s *Scheduler) Wait() {
	s.retry.wait()
	s.scanner.wait()
	s.task.wait()
}
