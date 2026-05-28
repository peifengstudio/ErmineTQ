package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// CreateTaskInput holds caller-supplied fields for a new task.
// ID, Status, RetryCount, CreatedAt, and UpdatedAt are set by CreateTask.
type CreateTaskInput struct {
	Type        string
	Queue       string          // "" → "default"
	Payload     json.RawMessage // nil → NULL
	Priority    int
	MaxRetries  int       // 0 → 3
	TimeoutSecs *int      // nil → no timeout
	RunAt       time.Time // zero → time.Now()
	ParentID    *string   // nil → not a restart
	ScheduleID  *string   // nil → not from a schedule
}

// CreateTask inserts a new task with status "queued" and records a "queued"
// task_event in the same transaction.
// Defaults: Queue → "default", MaxRetries → 3, RunAt → now.
func (s *Store) CreateTask(ctx context.Context, in CreateTaskInput) (*Task, error) {
	now := time.Now().UTC()

	t := &Task{
		ID:          NewID(),
		Type:        in.Type,
		Queue:       in.Queue,
		Payload:     in.Payload,
		Status:      TaskStatusQueued,
		Priority:    in.Priority,
		RetryCount:  0,
		MaxRetries:  in.MaxRetries,
		TimeoutSecs: in.TimeoutSecs,
		RunAt:       in.RunAt,
		ParentID:    in.ParentID,
		ScheduleID:  in.ScheduleID,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	if t.Queue == "" {
		t.Queue = "default"
	}
	if t.MaxRetries == 0 {
		t.MaxRetries = 3
	}
	if t.RunAt.IsZero() {
		t.RunAt = now
	}

	err := s.write(ctx, func(tx *sql.Tx) error {
		if _, err := tx.Exec(`
			INSERT INTO tasks
				(id, type, queue, payload, status, priority, retry_count, max_retries,
				 timeout_secs, run_at, parent_id, schedule_id, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			t.ID, t.Type, t.Queue,
			[]byte(t.Payload), string(t.Status),
			t.Priority, t.RetryCount, t.MaxRetries,
			t.TimeoutSecs, fmtDBTime(t.RunAt),
			t.ParentID, t.ScheduleID,
			fmtDBTime(t.CreatedAt), fmtDBTime(t.UpdatedAt),
		); err != nil {
			return err
		}
		_, err := tx.Exec(`
			INSERT INTO task_events (id, task_id, event, created_at)
			VALUES (?, ?, ?, ?)`,
			NewID(), t.ID, string(TaskEventQueued), fmtDBTime(now),
		)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("CreateTask: %w", err)
	}
	return t, nil
}

// GetTask returns the task with the given ID along with its full execution
// history (attempts ordered by attempt_num ASC, events ordered by created_at ASC).
// Returns ErrNotFound if no such task exists.
func (s *Store) GetTask(ctx context.Context, id string) (*Task, []*Attempt, []*TaskEvent, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, type, queue, payload, status, priority, retry_count, max_retries,
		       timeout_secs, run_at, parent_id, schedule_id, created_at, updated_at
		FROM tasks
		WHERE id = ?`, id)
	t, err := scanTask(row.Scan)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil, nil, ErrNotFound
	}
	if err != nil {
		return nil, nil, nil, fmt.Errorf("GetTask: %w", err)
	}

	attempts, err := s.listAttemptsByTask(ctx, id)
	if err != nil {
		return nil, nil, nil, err
	}

	events, err := s.listEventsByTask(ctx, id)
	if err != nil {
		return nil, nil, nil, err
	}

	return t, attempts, events, nil
}

// ListTasksFilter restricts the tasks returned by ListTasks.
// Nil filter fields match all values; Limit 0 defaults to 50.
type ListTasksFilter struct {
	Status *TaskStatus
	Type   *string
	Queue  *string
	Limit  int
	Offset int
}

// ListTasks returns tasks matching the filter, ordered by priority DESC then
// created_at ASC.
func (s *Store) ListTasks(ctx context.Context, f ListTasksFilter) ([]*Task, error) {
	limit := f.Limit
	if limit <= 0 {
		limit = 50
	}

	query := `
		SELECT id, type, queue, payload, status, priority, retry_count, max_retries,
		       timeout_secs, run_at, parent_id, schedule_id, created_at, updated_at
		FROM tasks
		WHERE 1=1`
	args := make([]any, 0, 5)

	if f.Status != nil {
		query += ` AND status = ?`
		args = append(args, string(*f.Status))
	}
	if f.Type != nil {
		query += ` AND type = ?`
		args = append(args, *f.Type)
	}
	if f.Queue != nil {
		query += ` AND queue = ?`
		args = append(args, *f.Queue)
	}

	query += ` ORDER BY priority DESC, created_at ASC LIMIT ? OFFSET ?`
	args = append(args, limit, f.Offset)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("ListTasks: %w", err)
	}
	defer rows.Close()

	var tasks []*Task
	for rows.Next() {
		t, err := scanTask(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("ListTasks scan: %w", err)
		}
		tasks = append(tasks, t)
	}
	return tasks, rows.Err()
}

// listAttemptsByTask returns all attempts for taskID, ordered by attempt_num ASC.
func (s *Store) listAttemptsByTask(ctx context.Context, taskID string) ([]*Attempt, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, task_id, attempt_num, worker_id, status, result, error,
		       started_at, finished_at, heartbeat_at
		FROM attempts
		WHERE task_id = ?
		ORDER BY attempt_num ASC`, taskID)
	if err != nil {
		return nil, fmt.Errorf("listAttemptsByTask: %w", err)
	}
	defer rows.Close()

	var attempts []*Attempt
	for rows.Next() {
		a, err := scanAttempt(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("listAttemptsByTask scan: %w", err)
		}
		attempts = append(attempts, a)
	}
	return attempts, rows.Err()
}

// listEventsByTask returns all task_events for taskID, ordered by created_at ASC.
func (s *Store) listEventsByTask(ctx context.Context, taskID string) ([]*TaskEvent, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, task_id, event, detail, created_at
		FROM task_events
		WHERE task_id = ?
		ORDER BY created_at ASC`, taskID)
	if err != nil {
		return nil, fmt.Errorf("listEventsByTask: %w", err)
	}
	defer rows.Close()

	var events []*TaskEvent
	for rows.Next() {
		e, err := scanTaskEvent(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("listEventsByTask scan: %w", err)
		}
		events = append(events, e)
	}
	return events, rows.Err()
}

// ── scan helpers ─────────────────────────────────────────────────────────────

func scanTask(scan func(...any) error) (*Task, error) {
	var (
		t           Task
		payload     []byte
		status      string
		timeoutSecs sql.NullInt64
		runAt       string
		parentID    sql.NullString
		scheduleID  sql.NullString
		createdAt   string
		updatedAt   string
	)
	if err := scan(
		&t.ID, &t.Type, &t.Queue,
		&payload, &status,
		&t.Priority, &t.RetryCount, &t.MaxRetries,
		&timeoutSecs, &runAt, &parentID, &scheduleID,
		&createdAt, &updatedAt,
	); err != nil {
		return nil, err
	}

	t.Payload = json.RawMessage(payload)
	t.Status = TaskStatus(status)

	if timeoutSecs.Valid {
		v := int(timeoutSecs.Int64)
		t.TimeoutSecs = &v
	}
	if parentID.Valid {
		t.ParentID = &parentID.String
	}
	if scheduleID.Valid {
		t.ScheduleID = &scheduleID.String
	}

	var err error
	if t.RunAt, err = parseDBTime(runAt); err != nil {
		return nil, fmt.Errorf("run_at: %w", err)
	}
	if t.CreatedAt, err = parseDBTime(createdAt); err != nil {
		return nil, fmt.Errorf("created_at: %w", err)
	}
	if t.UpdatedAt, err = parseDBTime(updatedAt); err != nil {
		return nil, fmt.Errorf("updated_at: %w", err)
	}
	return &t, nil
}

func scanAttempt(scan func(...any) error) (*Attempt, error) {
	var (
		a           Attempt
		workerID    sql.NullString
		status      string
		result      []byte
		errMsg      sql.NullString
		startedAt   sql.NullString
		finishedAt  sql.NullString
		heartbeatAt sql.NullString
	)
	if err := scan(
		&a.ID, &a.TaskID, &a.AttemptNum,
		&workerID, &status, &result, &errMsg,
		&startedAt, &finishedAt, &heartbeatAt,
	); err != nil {
		return nil, err
	}

	a.Status = AttemptStatus(status)
	a.Result = json.RawMessage(result)

	if workerID.Valid {
		a.WorkerID = &workerID.String
	}
	if errMsg.Valid {
		a.Error = &errMsg.String
	}

	var err error
	if a.StartedAt, err = parseNullDBTime(startedAt); err != nil {
		return nil, fmt.Errorf("started_at: %w", err)
	}
	if a.FinishedAt, err = parseNullDBTime(finishedAt); err != nil {
		return nil, fmt.Errorf("finished_at: %w", err)
	}
	if a.HeartbeatAt, err = parseNullDBTime(heartbeatAt); err != nil {
		return nil, fmt.Errorf("heartbeat_at: %w", err)
	}
	return &a, nil
}

func scanTaskEvent(scan func(...any) error) (*TaskEvent, error) {
	var (
		e         TaskEvent
		event     string
		detail    []byte
		createdAt string
	)
	if err := scan(&e.ID, &e.TaskID, &event, &detail, &createdAt); err != nil {
		return nil, err
	}

	e.Event = TaskEventType(event)
	e.Detail = json.RawMessage(detail)

	var err error
	if e.CreatedAt, err = parseDBTime(createdAt); err != nil {
		return nil, fmt.Errorf("created_at: %w", err)
	}
	return &e, nil
}
