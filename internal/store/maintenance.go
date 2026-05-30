package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

// RequeueRetrying promotes all retrying tasks whose run_at is in the past back
// to queued status. Called periodically by the scheduler.  Returns the number
// of tasks transitioned.
func (s *Store) RequeueRetrying(ctx context.Context) (int, error) {
	var n int64
	err := s.write(ctx, func(tx *sql.Tx) error {
		now := fmtDBTime(time.Now().UTC())
		res, err := tx.Exec(`
			UPDATE tasks
			SET    status = 'queued', updated_at = ?
			WHERE  status = 'retrying'
			  AND  run_at <= ?`,
			now, now,
		)
		if err != nil {
			return fmt.Errorf("RequeueRetrying: %w", err)
		}
		n, err = res.RowsAffected()
		return err
	})
	return int(n), err
}

// FindStaleAttempts returns all running attempts whose last heartbeat (or
// started_at if no heartbeat has been recorded yet) is at or before cutoff.
// The caller uses this list to decide which attempts to time out.
func (s *Store) FindStaleAttempts(ctx context.Context, cutoff time.Time) ([]*Attempt, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, task_id, attempt_num, worker_id, status, result, error,
		       started_at, finished_at, heartbeat_at
		FROM   attempts
		WHERE  status = 'running'
		  AND  COALESCE(heartbeat_at, started_at) <= ?`,
		fmtDBTime(cutoff.UTC()),
	)
	if err != nil {
		return nil, fmt.Errorf("FindStaleAttempts: %w", err)
	}
	defer rows.Close()

	var attempts []*Attempt
	for rows.Next() {
		a, err := scanAttempt(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("FindStaleAttempts scan: %w", err)
		}
		attempts = append(attempts, a)
	}
	return attempts, rows.Err()
}

// TimeoutAttempt is identical to FailAttempt except that the task_event type
// is always heartbeat_timeout (instead of retrying/dead) to distinguish
// timeouts from normal execution failures in the dashboard timeline.
func (s *Store) TimeoutAttempt(ctx context.Context, attemptID string) error {
	const errMsg = "heartbeat timeout"
	var capturedTaskID string
	err := s.write(ctx, func(tx *sql.Tx) error {
		now := time.Now().UTC()
		nowStr := fmtDBTime(now)

		taskID, workerID, err := loadRunningAttempt(tx, attemptID)
		if err != nil {
			return err
		}
		capturedTaskID = taskID

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

		var detail []byte
		if retryCount < maxRetries {
			runAt := fmtDBTime(now.Add(retryBackoff(retryCount)))
			if _, err := tx.Exec(`
				UPDATE tasks
				SET    status = 'retrying', retry_count = retry_count + 1,
				       run_at = ?, updated_at = ?
				WHERE  id = ?`,
				runAt, nowStr, taskID,
			); err != nil {
				return fmt.Errorf("update task (retrying): %w", err)
			}
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

		return insertTaskEvent(tx, taskID, TaskEventHeartbeatTimeout, detail, nowStr)
	})
	if err != nil {
		return err
	}
	s.emit(SSEEvent{TaskID: capturedTaskID, Event: TaskEventHeartbeatTimeout})
	return nil
}

// DueSchedules returns all enabled schedules whose next_run_at is in the past
// (or exactly now). The scheduler uses this to fire new task instances.
func (s *Store) DueSchedules(ctx context.Context) ([]*Schedule, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, task_type, queue, payload, cron_expr, interval_secs,
		       enabled, last_run_at, next_run_at, created_at
		FROM   schedules
		WHERE  enabled = 1
		  AND  next_run_at <= ?`,
		fmtDBTime(time.Now().UTC()),
	)
	if err != nil {
		return nil, fmt.Errorf("DueSchedules: %w", err)
	}
	defer rows.Close()

	var schedules []*Schedule
	for rows.Next() {
		sc, err := scanSchedule(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("DueSchedules scan: %w", err)
		}
		schedules = append(schedules, sc)
	}
	return schedules, rows.Err()
}

// RecordScheduleRun updates a schedule after a task has been dispatched:
// last_run_at is set to now and next_run_at is advanced to the provided value.
func (s *Store) RecordScheduleRun(ctx context.Context, id string, nextRunAt time.Time) error {
	return s.write(ctx, func(tx *sql.Tx) error {
		now := fmtDBTime(time.Now().UTC())
		res, err := tx.Exec(`
			UPDATE schedules
			SET    last_run_at = ?, next_run_at = ?
			WHERE  id = ?`,
			now, fmtDBTime(nextRunAt.UTC()), id,
		)
		if err != nil {
			return fmt.Errorf("RecordScheduleRun: %w", err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return ErrNotFound
		}
		return nil
	})
}
