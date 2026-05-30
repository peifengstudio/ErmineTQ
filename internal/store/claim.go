package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/peifengstudio/erminetq/internal/config"
)

// ClaimTask atomically acquires one queued task for the given worker, subject
// to the execution limits in cfg.  The caller (worker pool) must verify it has
// spare capacity before invoking this method.
//
// A single transaction performs, in order:
//  1. Global running-count guard (cfg.Limits.Global)
//  2. Per-queue running-count guard (cfg.QueueLimit)
//  3. Per-type running-count guard (cfg.TaskTypeLimit), filtering down the
//     eligible type list so types already at their limit are skipped
//  4. Atomic UPDATE … WHERE id=(SELECT … LIMIT 1) RETURNING * — claims the
//     highest-priority queued task among the eligible types
//  5. INSERT into attempts (attempt_num = prev_max + 1, status = running)
//  6. UPDATE workers: increment current_task_count, flip to 'busy' if full
//  7. INSERT into task_events (event = started, detail = {attempt_id})
//
// Returns (nil, "", nil) when no eligible task is available — this is not an
// error.  On success the returned string is the ID of the newly created attempt
// row; callers need this to report the execution outcome.
func (s *Store) ClaimTask(
	ctx context.Context,
	workerID, queue string,
	taskTypes []string,
	cfg *config.Config,
) (*Task, string, error) {
	if len(taskTypes) == 0 {
		return nil, "", nil
	}

	var claimed *Task
	var claimedAttemptID string

	err := s.write(ctx, func(tx *sql.Tx) error {
		now := fmtDBTime(time.Now().UTC())

		// ── Guard 1: global running count ────────────────────────────────────
		var globalRunning int
		if err := tx.QueryRow(
			`SELECT COUNT(*) FROM tasks WHERE status = 'running'`,
		).Scan(&globalRunning); err != nil {
			return fmt.Errorf("global count: %w", err)
		}
		if globalRunning >= cfg.Limits.Global {
			return nil
		}

		// ── Guard 2: per-queue running count ─────────────────────────────────
		if qLimit := cfg.QueueLimit(queue); qLimit > 0 {
			var qRunning int
			if err := tx.QueryRow(
				`SELECT COUNT(*) FROM tasks WHERE status = 'running' AND queue = ?`,
				queue,
			).Scan(&qRunning); err != nil {
				return fmt.Errorf("queue count: %w", err)
			}
			if qRunning >= qLimit {
				return nil
			}
		}

		// ── Guard 3: per-type running count ───────────────────────────────────
		// Build the set of types that still have capacity.
		eligible := make([]string, 0, len(taskTypes))
		for _, tt := range taskTypes {
			if tLimit := cfg.TaskTypeLimit(tt); tLimit > 0 {
				var tRunning int
				if err := tx.QueryRow(
					`SELECT COUNT(*) FROM tasks WHERE status = 'running' AND type = ?`,
					tt,
				).Scan(&tRunning); err != nil {
					return fmt.Errorf("type count %q: %w", tt, err)
				}
				if tRunning >= tLimit {
					continue
				}
			}
			eligible = append(eligible, tt)
		}
		if len(eligible) == 0 {
			return nil
		}

		// ── Atomic claim ──────────────────────────────────────────────────────
		ph := make([]string, len(eligible))
		for i := range ph {
			ph[i] = "?"
		}

		claimArgs := make([]any, 0, 3+len(eligible))
		claimArgs = append(claimArgs, now) // updated_at
		claimArgs = append(claimArgs, queue)
		for _, tt := range eligible {
			claimArgs = append(claimArgs, tt)
		}
		claimArgs = append(claimArgs, now) // run_at ≤ now

		claimRow := tx.QueryRow(`
			UPDATE tasks
			SET    status     = 'running',
			       updated_at = ?
			WHERE  id = (
			    SELECT id FROM tasks
			    WHERE  status = 'queued'
			      AND  queue  = ?
			      AND  type   IN (`+strings.Join(ph, ", ")+`)
			      AND  run_at <= ?
			    ORDER BY priority DESC, created_at ASC
			    LIMIT 1
			)
			RETURNING id, type, queue, payload, status, priority, retry_count, max_retries,
			          timeout_secs, run_at, parent_id, schedule_id, created_at, updated_at`,
			claimArgs...,
		)

		t, err := scanTask(claimRow.Scan)
		if errors.Is(err, sql.ErrNoRows) {
			return nil // queue empty or no task matched
		}
		if err != nil {
			return fmt.Errorf("claim UPDATE: %w", err)
		}
		claimed = t

		// ── Insert attempt ────────────────────────────────────────────────────
		attemptID := NewID()
		claimedAttemptID = attemptID
		var prevMax int
		if err := tx.QueryRow(
			`SELECT COALESCE(MAX(attempt_num), 0) FROM attempts WHERE task_id = ?`,
			claimed.ID,
		).Scan(&prevMax); err != nil {
			return fmt.Errorf("attempt_num query: %w", err)
		}

		if _, err := tx.Exec(`
			INSERT INTO attempts (id, task_id, attempt_num, worker_id, status, started_at)
			VALUES (?, ?, ?, ?, 'running', ?)`,
			attemptID, claimed.ID, prevMax+1, workerID, now,
		); err != nil {
			return fmt.Errorf("insert attempt: %w", err)
		}

		// ── Update worker count ───────────────────────────────────────────────
		// current_task_count + 1 evaluated against original row value → new count.
		if _, err := tx.Exec(`
			UPDATE workers
			SET current_task_count = current_task_count + 1,
			    status = CASE WHEN current_task_count + 1 >= concurrency
			                  THEN 'busy' ELSE 'idle' END
			WHERE id = ?`, workerID,
		); err != nil {
			return fmt.Errorf("update worker: %w", err)
		}

		// ── Write started event ───────────────────────────────────────────────
		detail, _ := json.Marshal(map[string]string{"attempt_id": attemptID})
		if _, err := tx.Exec(`
			INSERT INTO task_events (id, task_id, event, detail, created_at)
			VALUES (?, ?, 'started', ?, ?)`,
			NewID(), claimed.ID, string(detail), now,
		); err != nil {
			return fmt.Errorf("insert started event: %w", err)
		}

		return nil
	})
	if err != nil {
		return nil, "", err
	}
	if claimed != nil {
		s.emit(SSEEvent{TaskID: claimed.ID, Event: TaskEventStarted})
	}
	return claimed, claimedAttemptID, nil
}
