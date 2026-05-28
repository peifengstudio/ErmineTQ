package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// ErrAttemptNotRunning is returned when an outcome method is called on an
// attempt that is not currently in 'running' status.
var ErrAttemptNotRunning = errors.New("attempt is not running")

// Exponential backoff parameters for FailAttempt retry scheduling.
const (
	retryBaseDelay time.Duration = 30 * time.Second
	retryMaxDelay  time.Duration = time.Hour
)

// retryBackoff returns the delay to wait before re-queuing a retrying task.
// Formula: base × 2^retryCount, capped at retryMaxDelay.
// retryCount is the value read from the task row BEFORE incrementing.
//
// Example progression (base=30s):
//
//	retry 0 → 30s   retry 1 → 60s   retry 2 → 120s
//	retry 3 → 240s  retry 4 → 480s  retry 5 → 960s  retry ≥ 7 → 1h
func retryBackoff(retryCount int) time.Duration {
	const maxShift = 7 // 30s×2^7=3840s > 3600s cap; avoids int64 overflow
	if retryCount < 0 {
		retryCount = 0
	}
	if retryCount > maxShift {
		retryCount = maxShift
	}
	d := retryBaseDelay * time.Duration(1<<uint(retryCount))
	if d > retryMaxDelay {
		return retryMaxDelay
	}
	return d
}

// SucceedAttempt records a successful execution outcome in a single transaction:
//   - attempt  → succeeded (result stored, finished_at set)
//   - task     → succeeded
//   - worker   current_task_count decremented
//   - task_events ← 'succeeded' {attempt_id}
func (s *Store) SucceedAttempt(ctx context.Context, attemptID string, result json.RawMessage) error {
	return s.write(ctx, func(tx *sql.Tx) error {
		now := fmtDBTime(time.Now().UTC())

		taskID, workerID, err := loadRunningAttempt(tx, attemptID)
		if err != nil {
			return err
		}

		if _, err := tx.Exec(`
			UPDATE attempts
			SET status = 'succeeded', result = ?, finished_at = ?
			WHERE id = ?`,
			[]byte(result), now, attemptID,
		); err != nil {
			return fmt.Errorf("update attempt: %w", err)
		}

		if _, err := tx.Exec(`
			UPDATE tasks SET status = 'succeeded', updated_at = ? WHERE id = ?`,
			now, taskID,
		); err != nil {
			return fmt.Errorf("update task: %w", err)
		}

		if workerID != "" {
			if err := decrementWorker(tx, workerID); err != nil {
				return err
			}
		}

		detail, _ := json.Marshal(map[string]string{"attempt_id": attemptID})
		_, err = tx.Exec(`
			INSERT INTO task_events (id, task_id, event, detail, created_at)
			VALUES (?, ?, 'succeeded', ?, ?)`,
			NewID(), taskID, string(detail), now,
		)
		return err
	})
}

// FailAttempt records a failed execution outcome in a single transaction:
//   - attempt  → failed (error stored, finished_at set)
//   - if retry_count < max_retries: task → retrying (run_at += backoff, retry_count++)
//   - otherwise:                    task → dead
//   - worker   current_task_count decremented
//   - task_events ← 'retrying' or 'dead' with error and attempt_id
func (s *Store) FailAttempt(ctx context.Context, attemptID, errMsg string) error {
	return s.write(ctx, func(tx *sql.Tx) error {
		now := time.Now().UTC()
		nowStr := fmtDBTime(now)

		taskID, workerID, err := loadRunningAttempt(tx, attemptID)
		if err != nil {
			return err
		}

		var retryCount, maxRetries int
		if err := tx.QueryRow(
			`SELECT retry_count, max_retries FROM tasks WHERE id = ?`, taskID,
		).Scan(&retryCount, &maxRetries); err != nil {
			return fmt.Errorf("read task: %w", err)
		}

		if _, err := tx.Exec(`
			UPDATE attempts SET status = 'failed', error = ?, finished_at = ? WHERE id = ?`,
			errMsg, nowStr, attemptID,
		); err != nil {
			return fmt.Errorf("update attempt: %w", err)
		}

		var eventType TaskEventType
		var detail []byte

		if retryCount < maxRetries {
			runAt := fmtDBTime(now.Add(retryBackoff(retryCount)))
			if _, err := tx.Exec(`
				UPDATE tasks
				SET status = 'retrying', retry_count = retry_count + 1,
				    run_at = ?, updated_at = ?
				WHERE id = ?`,
				runAt, nowStr, taskID,
			); err != nil {
				return fmt.Errorf("update task (retrying): %w", err)
			}
			eventType = TaskEventRetrying
			detail, _ = json.Marshal(map[string]any{
				"attempt_id":  attemptID,
				"error":       errMsg,
				"retry_count": retryCount + 1,
			})
		} else {
			if _, err := tx.Exec(`
				UPDATE tasks SET status = 'dead', updated_at = ? WHERE id = ?`,
				nowStr, taskID,
			); err != nil {
				return fmt.Errorf("update task (dead): %w", err)
			}
			eventType = TaskEventDead
			detail, _ = json.Marshal(map[string]any{
				"attempt_id": attemptID,
				"error":      errMsg,
			})
		}

		if workerID != "" {
			if err := decrementWorker(tx, workerID); err != nil {
				return err
			}
		}

		_, err = tx.Exec(`
			INSERT INTO task_events (id, task_id, event, detail, created_at)
			VALUES (?, ?, ?, ?, ?)`,
			NewID(), taskID, string(eventType), string(detail), nowStr,
		)
		return err
	})
}

// CancelAttempt records a cancelled execution outcome in a single transaction:
//   - attempt  → cancelled (finished_at set)
//   - task     → cancelled
//   - worker   current_task_count decremented
//   - task_events ← 'cancelled' {attempt_id}
func (s *Store) CancelAttempt(ctx context.Context, attemptID string) error {
	return s.write(ctx, func(tx *sql.Tx) error {
		now := fmtDBTime(time.Now().UTC())

		taskID, workerID, err := loadRunningAttempt(tx, attemptID)
		if err != nil {
			return err
		}

		if _, err := tx.Exec(`
			UPDATE attempts SET status = 'cancelled', finished_at = ? WHERE id = ?`,
			now, attemptID,
		); err != nil {
			return fmt.Errorf("update attempt: %w", err)
		}

		if _, err := tx.Exec(`
			UPDATE tasks SET status = 'cancelled', updated_at = ? WHERE id = ?`,
			now, taskID,
		); err != nil {
			return fmt.Errorf("update task: %w", err)
		}

		if workerID != "" {
			if err := decrementWorker(tx, workerID); err != nil {
				return err
			}
		}

		detail, _ := json.Marshal(map[string]string{"attempt_id": attemptID})
		_, err = tx.Exec(`
			INSERT INTO task_events (id, task_id, event, detail, created_at)
			VALUES (?, ?, 'cancelled', ?, ?)`,
			NewID(), taskID, string(detail), now,
		)
		return err
	})
}

// UpdateHeartbeat records that a running attempt is still alive by updating
// attempts.heartbeat_at.  Intentionally produces no task_events entry.
// Silently succeeds if the attempt is not found or no longer running —
// races between heartbeat and completion are normal and benign.
func (s *Store) UpdateHeartbeat(ctx context.Context, attemptID string) error {
	return s.write(ctx, func(tx *sql.Tx) error {
		_, err := tx.Exec(
			`UPDATE attempts SET heartbeat_at = ? WHERE id = ? AND status = 'running'`,
			fmtDBTime(time.Now().UTC()), attemptID,
		)
		return err
	})
}

// ── private helpers ───────────────────────────────────────────────────────────

// loadRunningAttempt reads task_id and worker_id from the attempt row,
// asserting that its status is 'running'.
// Returns ErrNotFound or ErrAttemptNotRunning on precondition failures.
func loadRunningAttempt(tx *sql.Tx, attemptID string) (taskID, workerID string, err error) {
	var workerNullStr sql.NullString
	var status string
	scanErr := tx.QueryRow(
		`SELECT task_id, worker_id, status FROM attempts WHERE id = ?`, attemptID,
	).Scan(&taskID, &workerNullStr, &status)
	if errors.Is(scanErr, sql.ErrNoRows) {
		return "", "", ErrNotFound
	}
	if scanErr != nil {
		return "", "", fmt.Errorf("read attempt: %w", scanErr)
	}
	if AttemptStatus(status) != AttemptStatusRunning {
		return "", "", ErrAttemptNotRunning
	}
	if workerNullStr.Valid {
		workerID = workerNullStr.String
	}
	return taskID, workerID, nil
}

// decrementWorker decrements current_task_count for the given worker and
// sets status to 'idle' when the new count drops below concurrency.
func decrementWorker(tx *sql.Tx, workerID string) error {
	_, err := tx.Exec(`
		UPDATE workers
		SET current_task_count = current_task_count - 1,
		    status = CASE WHEN current_task_count - 1 < concurrency
		                  THEN 'idle' ELSE 'busy' END
		WHERE id = ?`, workerID,
	)
	if err != nil {
		return fmt.Errorf("decrement worker: %w", err)
	}
	return nil
}
