package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// ErrInvalidTransition is returned when a control action is attempted but the
// task is not in a permitted source state.
var ErrInvalidTransition = errors.New("invalid state transition")

// HaltTask transitions a running task to halted state and records a 'halted'
// event.  The running attempt (if any) is left in 'running'; the worker is
// expected to call CancelAttempt cooperatively when it detects the halt.
//
// Valid source state: running.
func (s *Store) HaltTask(ctx context.Context, id string) error {
	return s.controlTransition(ctx, id,
		[]TaskStatus{TaskStatusRunning},
		TaskStatusHalted,
		TaskEventHalted,
	)
}

// ResumeTask transitions a halted task back to queued, resetting run_at to now
// so it becomes immediately eligible for claiming.
//
// Valid source state: halted.
func (s *Store) ResumeTask(ctx context.Context, id string) error {
	return s.write(ctx, func(tx *sql.Tx) error {
		now := fmtDBTime(time.Now().UTC())

		status, err := taskStatusTx(tx, id)
		if err != nil {
			return err
		}
		if status != TaskStatusHalted {
			return fmt.Errorf("ResumeTask: task is %q, want halted: %w", status, ErrInvalidTransition)
		}

		if _, err := tx.Exec(`
			UPDATE tasks SET status = 'queued', run_at = ?, updated_at = ? WHERE id = ?`,
			now, now, id,
		); err != nil {
			return fmt.Errorf("update task: %w", err)
		}
		return insertTaskEvent(tx, id, TaskEventQueued, nil, now)
	})
}

// CancelTask transitions a queued or halted task to cancelled.
// For a running task, call HaltTask first and then CancelTask once the attempt
// has been resolved cooperatively.
//
// Valid source states: queued, halted.
func (s *Store) CancelTask(ctx context.Context, id string) error {
	return s.controlTransition(ctx, id,
		[]TaskStatus{TaskStatusQueued, TaskStatusHalted},
		TaskStatusCancelled,
		TaskEventCancelled,
	)
}

// RetryTask transitions a dead or cancelled task back to queued, resetting
// retry_count to zero and run_at to now.
//
// Valid source states: dead, cancelled.
func (s *Store) RetryTask(ctx context.Context, id string) error {
	return s.write(ctx, func(tx *sql.Tx) error {
		now := fmtDBTime(time.Now().UTC())

		status, err := taskStatusTx(tx, id)
		if err != nil {
			return err
		}
		if status != TaskStatusDead && status != TaskStatusCancelled {
			return fmt.Errorf("RetryTask: task is %q, want dead or cancelled: %w",
				status, ErrInvalidTransition)
		}

		if _, err := tx.Exec(`
			UPDATE tasks
			SET status = 'queued', retry_count = 0, run_at = ?, updated_at = ?
			WHERE id = ?`,
			now, now, id,
		); err != nil {
			return fmt.Errorf("update task: %w", err)
		}
		return insertTaskEvent(tx, id, TaskEventQueued, nil, now)
	})
}

// RestartTask supersedes the original task and creates a new queued task that
// copies all configuration fields (type, queue, payload, priority, max_retries,
// timeout_secs, schedule_id) with parent_id set to the original task's ID.
//
// The new task is returned on success.
//
// Valid source states: queued, succeeded, dead, cancelled, halted.
// Running and retrying tasks must have their active attempt resolved first;
// superseded tasks cannot be restarted again.
func (s *Store) RestartTask(ctx context.Context, id string) (*Task, error) {
	var newTask *Task

	err := s.write(ctx, func(tx *sql.Tx) error {
		now := time.Now().UTC()
		nowStr := fmtDBTime(now)

		// Read the original task inside the transaction.
		orig, err := readTaskTx(tx, id)
		if err != nil {
			return err
		}

		// Validate source state.
		switch orig.Status {
		case TaskStatusQueued, TaskStatusSucceeded,
			TaskStatusDead, TaskStatusCancelled, TaskStatusHalted:
			// allowed
		default:
			return fmt.Errorf("RestartTask: task is %q, cannot restart from this state: %w",
				orig.Status, ErrInvalidTransition)
		}

		// Supersede the original.
		if _, err := tx.Exec(
			`UPDATE tasks SET status = 'superseded', updated_at = ? WHERE id = ?`,
			nowStr, id,
		); err != nil {
			return fmt.Errorf("supersede original: %w", err)
		}

		// Build the replacement task.
		nt := &Task{
			ID:          NewID(),
			Type:        orig.Type,
			Queue:       orig.Queue,
			Payload:     orig.Payload,
			Status:      TaskStatusQueued,
			Priority:    orig.Priority,
			RetryCount:  0,
			MaxRetries:  orig.MaxRetries,
			TimeoutSecs: orig.TimeoutSecs,
			RunAt:       now,
			ParentID:    &id,
			ScheduleID:  orig.ScheduleID,
			CreatedAt:   now,
			UpdatedAt:   now,
		}

		if _, err := tx.Exec(`
			INSERT INTO tasks
				(id, type, queue, payload, status, priority, retry_count, max_retries,
				 timeout_secs, run_at, parent_id, schedule_id, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			nt.ID, nt.Type, nt.Queue,
			[]byte(nt.Payload), string(nt.Status),
			nt.Priority, nt.RetryCount, nt.MaxRetries,
			nt.TimeoutSecs, fmtDBTime(nt.RunAt),
			nt.ParentID, nt.ScheduleID,
			fmtDBTime(nt.CreatedAt), fmtDBTime(nt.UpdatedAt),
		); err != nil {
			return fmt.Errorf("insert new task: %w", err)
		}

		// Queued event for the new task.
		if err := insertTaskEvent(tx, nt.ID, TaskEventQueued, nil, nowStr); err != nil {
			return err
		}

		newTask = nt
		return nil
	})
	if err != nil {
		return nil, err
	}
	return newTask, nil
}

// ── private helpers ───────────────────────────────────────────────────────────

// controlTransition handles the common pattern of: read status → assert source
// state → UPDATE status → INSERT event.  Used by HaltTask and CancelTask.
func (s *Store) controlTransition(
	ctx context.Context,
	id string,
	from []TaskStatus,
	to TaskStatus,
	event TaskEventType,
) error {
	return s.write(ctx, func(tx *sql.Tx) error {
		now := fmtDBTime(time.Now().UTC())

		status, err := taskStatusTx(tx, id)
		if err != nil {
			return err
		}
		if !taskStatusOneOf(status, from) {
			return fmt.Errorf("task is %q, want one of %v: %w", status, from, ErrInvalidTransition)
		}

		if _, err := tx.Exec(
			`UPDATE tasks SET status = ?, updated_at = ? WHERE id = ?`,
			string(to), now, id,
		); err != nil {
			return fmt.Errorf("update task: %w", err)
		}
		return insertTaskEvent(tx, id, event, nil, now)
	})
}

// taskStatusTx reads a task's current status inside an open transaction.
// Returns ErrNotFound if the task does not exist.
func taskStatusTx(tx *sql.Tx, id string) (TaskStatus, error) {
	var s string
	err := tx.QueryRow(`SELECT status FROM tasks WHERE id = ?`, id).Scan(&s)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrNotFound
	}
	if err != nil {
		return "", fmt.Errorf("read task status: %w", err)
	}
	return TaskStatus(s), nil
}

// readTaskTx reads a complete task row inside an open transaction.
// Returns ErrNotFound if the task does not exist.
func readTaskTx(tx *sql.Tx, id string) (*Task, error) {
	row := tx.QueryRow(`
		SELECT id, type, queue, payload, status, priority, retry_count, max_retries,
		       timeout_secs, run_at, parent_id, schedule_id, created_at, updated_at
		FROM tasks WHERE id = ?`, id)
	t, err := scanTask(row.Scan)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return t, err
}

// insertTaskEvent inserts a task_events row inside an open transaction.
// detail may be nil (stored as SQL NULL).
func insertTaskEvent(tx *sql.Tx, taskID string, event TaskEventType, detail json.RawMessage, now string) error {
	_, err := tx.Exec(`
		INSERT INTO task_events (id, task_id, event, detail, created_at)
		VALUES (?, ?, ?, ?, ?)`,
		NewID(), taskID, string(event), []byte(detail), now,
	)
	if err != nil {
		return fmt.Errorf("insert %s event: %w", event, err)
	}
	return nil
}

// taskStatusOneOf reports whether status is in the allowed set.
func taskStatusOneOf(status TaskStatus, allowed []TaskStatus) bool {
	for _, a := range allowed {
		if status == a {
			return true
		}
	}
	return false
}
