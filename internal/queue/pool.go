package queue

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/peifengstudio/erminetq/internal/config"
	"github.com/peifengstudio/erminetq/internal/store"
)

const defaultPollInterval = 500 * time.Millisecond

// PoolConfig configures a Pool.
type PoolConfig struct {
	Store    TaskStore
	Registry *Registry

	// Config is forwarded to ClaimTask for concurrency-limit enforcement.
	Config *config.Config

	// Queue is the queue name this pool will drain.
	Queue string

	// Concurrency is the number of polling goroutines (and therefore the
	// maximum number of tasks that can run in parallel on this pool).
	// Zero or negative is clamped to 1.
	Concurrency int

	// PollInterval is the idle back-off duration: how long a goroutine sleeps
	// after ClaimTask returns nil before it tries again.  Zero uses 500 ms.
	PollInterval time.Duration

	// OnError is called for non-fatal errors returned by RunOnce that are not
	// caused by context cancellation.  Nil means errors are silently discarded.
	// Useful in tests and for structured logging in the server layer.
	OnError func(err error)

	// Bridge is reserved for Phase 5 Python Bridge integration.
	// Leave nil for pure-Go worker pools.
	// Bridge *bridge.Client
}

// Pool manages Concurrency polling goroutines that each independently run
// RunOnce in a tight loop, backing off PollInterval when the queue is empty.
type Pool struct {
	workerID string
	cfg      PoolConfig // normalised copy
	wg       sync.WaitGroup
}

// NewPool registers a worker record in the Store and returns a Pool ready to
// Start.  The ctx argument is only used for the registration call; pass the
// long-lived run context to Start.
func NewPool(ctx context.Context, cfg PoolConfig) (*Pool, error) {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaultPollInterval
	}

	w, err := cfg.Store.CreateWorker(ctx, store.CreateWorkerInput{
		Type:        store.WorkerTypeGo,
		TaskTypes:   cfg.Registry.TaskTypes(),
		Queue:       cfg.Queue,
		Concurrency: cfg.Concurrency,
	})
	if err != nil {
		return nil, fmt.Errorf("NewPool: register worker: %w", err)
	}

	return &Pool{
		workerID: w.ID,
		cfg:      cfg,
	}, nil
}

// Start spawns Concurrency goroutines that poll for tasks until ctx is
// cancelled.  Start is non-blocking; pair it with Wait to drain in-flight
// tasks after ctx is cancelled.
func (p *Pool) Start(ctx context.Context) {
	for i := 0; i < p.cfg.Concurrency; i++ {
		p.wg.Add(1)
		go p.poll(ctx)
	}
}

// Wait blocks until every goroutine spawned by Start has returned.  Always
// call this after cancelling the context passed to Start.
func (p *Pool) Wait() {
	p.wg.Wait()
}

// WorkerID returns the worker ID that was registered in the Store by NewPool.
func (p *Pool) WorkerID() string {
	return p.workerID
}

// poll is the inner loop run by each goroutine.
func (p *Pool) poll(ctx context.Context) {
	defer p.wg.Done()

	rc := RunConfig{
		Store:    p.cfg.Store,
		Registry: p.cfg.Registry,
		WorkerID: p.workerID,
		Queue:    p.cfg.Queue,
		Config:   p.cfg.Config,
	}

	for {
		// Bail out before the claim round-trip if already cancelled.
		if ctx.Err() != nil {
			return
		}

		got, err := RunOnce(ctx, rc)

		if err != nil {
			if ctx.Err() != nil {
				// Cancellation is the normal shutdown path, not an error.
				return
			}
			if p.cfg.OnError != nil {
				p.cfg.OnError(err)
			}
		}

		if !got {
			// Queue empty (or ctx cancelled): back off before the next poll.
			select {
			case <-ctx.Done():
				return
			case <-time.After(p.cfg.PollInterval):
			}
		}
	}
}
