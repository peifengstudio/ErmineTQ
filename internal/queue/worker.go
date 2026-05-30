package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/peifengstudio/erminetq/internal/config"
)

const defaultHeartbeatInterval = 5 * time.Second

// RunConfig holds everything RunOnce needs to execute one task.
type RunConfig struct {
	Store    TaskStore
	Registry *Registry

	// Identity used when claiming tasks and creating attempt rows.
	WorkerID string
	Queue    string

	// Concurrency limits forwarded to ClaimTask.
	Config *config.Config

	// HeartbeatInterval controls how often UpdateHeartbeat is called while a
	// task is executing.  Zero uses the default of 5 seconds.
	HeartbeatInterval time.Duration
}

// RunOnce claims one task from the queue and executes it to completion.
//
// Return values:
//   - (true,  nil) — a task was found and executed (success or handled failure)
//   - (false, nil) — queue is empty, no task was available
//   - (false, err) — infrastructure error (claim failed, outcome write failed,
//     or parent context was cancelled before a task could finish)
func RunOnce(ctx context.Context, cfg RunConfig) (bool, error) {
	interval := cfg.HeartbeatInterval
	if interval <= 0 {
		interval = defaultHeartbeatInterval
	}

	// Nothing registered → nothing to claim.
	taskTypes := cfg.Registry.TaskTypes()
	if len(taskTypes) == 0 {
		return false, nil
	}

	// ── 1. Claim ──────────────────────────────────────────────────────────────
	task, attemptID, err := cfg.Store.ClaimTask(ctx, cfg.WorkerID, cfg.Queue, taskTypes, cfg.Config)
	if err != nil {
		return false, fmt.Errorf("ClaimTask: %w", err)
	}
	if task == nil {
		return false, nil // queue empty or all limits saturated
	}

	// ── 2. Build execution context (respects timeout_secs + parent cancel) ────
	execCtx, execCancel := buildExecCtx(ctx, task.TimeoutSecs)
	defer execCancel()

	// ── 3. Heartbeat goroutine ─────────────────────────────────────────────────
	// Uses its own cancel so we can stop it independently of execCtx.
	hbCtx, hbCancel := context.WithCancel(context.Background())
	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				// Ignore errors: a stale heartbeat on an already-resolved
				// attempt is benign (store silently no-ops it).
				_ = cfg.Store.UpdateHeartbeat(context.Background(), attemptID)
			case <-hbCtx.Done():
				return
			}
		}
	}()

	// ── 4. Execute ────────────────────────────────────────────────────────────
	// Dispatch handles both Go WorkerFuncs and Python Bridge calls, and
	// converts panics to errors internally.
	result, execErr := cfg.Registry.Dispatch(execCtx, task)

	// Stop heartbeat before touching the store so there is no concurrent
	// UpdateHeartbeat racing against SucceedAttempt / FailAttempt.
	hbCancel()
	<-hbDone

	// ── 5. Report outcome ─────────────────────────────────────────────────────
	// Always use a fresh background context here: the parent may already be
	// cancelled (halt signal), but we still need to write the outcome.
	outcomeCtx := context.Background()

	switch {
	case ctx.Err() != nil:
		// Parent context cancelled → cooperative halt.
		if err := cfg.Store.CancelAttempt(outcomeCtx, attemptID); err != nil {
			return false, fmt.Errorf("CancelAttempt: %w", err)
		}
		return false, ctx.Err()

	case execErr != nil:
		// Worker returned an error (includes panic-derived errors and
		// deadline-exceeded when the task respects its context).
		if err := cfg.Store.FailAttempt(outcomeCtx, attemptID, execErr.Error()); err != nil {
			return false, fmt.Errorf("FailAttempt: %w", err)
		}
		return true, nil

	default:
		if err := cfg.Store.SucceedAttempt(outcomeCtx, attemptID, json.RawMessage(result)); err != nil {
			return false, fmt.Errorf("SucceedAttempt: %w", err)
		}
		return true, nil
	}
}

// ── private helpers ───────────────────────────────────────────────────────────

// buildExecCtx wraps parent with a timeout when the task specifies one,
// otherwise it just forwards parent's cancellation.
func buildExecCtx(parent context.Context, timeoutSecs *int) (context.Context, context.CancelFunc) {
	if timeoutSecs != nil && *timeoutSecs > 0 {
		return context.WithTimeout(parent, time.Duration(*timeoutSecs)*time.Second)
	}
	return context.WithCancel(parent)
}
