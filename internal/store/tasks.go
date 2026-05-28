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
	MaxRetries  int        // 0 → 3
	TimeoutSecs *int       // nil → no timeout
	RunAt       time.Time  // zero → time.Now()
	ParentID    *string    // nil → not a restart
	ScheduleID  *string    // nil → not from a schedule
}

// CreateTask inserts a new task with status "queued".
// Defaults are applied for Queue, MaxRetries, and RunAt when the zero value is
// provided.
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
		_, err := tx.Exec(`
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
		)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("CreateTask: %w", err)
	}
	return t, nil
}

// GetTask returns the task with the given ID.
// Returns ErrNotFound if no such task exists.
func (s *Store) GetTask(ctx context.Context, id string) (*Task, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, type, queue, payload, status, priority, retry_count, max_retries,
		       timeout_secs, run_at, parent_id, schedule_id, created_at, updated_at
		FROM tasks
		WHERE id = ?`, id)
	t, err := scanTask(row.Scan)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("GetTask: %w", err)
	}
	return t, nil
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

// scanTask reads one row from the tasks table into a Task.
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
