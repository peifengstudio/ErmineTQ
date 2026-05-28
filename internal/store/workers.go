package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// CreateWorkerInput holds caller-supplied fields for a new worker.
// ID, CurrentTaskCount, Status, and StartedAt are set by CreateWorker.
type CreateWorkerInput struct {
	Type        WorkerType
	TaskTypes   []string
	Queue       string // "" → "default"
	Concurrency int    // ≤0 → 1
}

// CreateWorker inserts a new worker with status "idle".
func (s *Store) CreateWorker(ctx context.Context, in CreateWorkerInput) (*Worker, error) {
	w := &Worker{
		ID:               NewID(),
		Type:             in.Type,
		TaskTypes:        in.TaskTypes,
		Queue:            in.Queue,
		Concurrency:      in.Concurrency,
		CurrentTaskCount: 0,
		Status:           WorkerStatusIdle,
		StartedAt:        time.Now().UTC(),
		HeartbeatAt:      nil,
	}
	if w.Queue == "" {
		w.Queue = "default"
	}
	if w.Concurrency <= 0 {
		w.Concurrency = 1
	}
	if w.TaskTypes == nil {
		w.TaskTypes = []string{}
	}

	taskTypesJSON, err := json.Marshal(w.TaskTypes)
	if err != nil {
		return nil, fmt.Errorf("CreateWorker marshal task_types: %w", err)
	}

	err = s.write(ctx, func(tx *sql.Tx) error {
		_, err := tx.Exec(`
			INSERT INTO workers
				(id, type, task_types, queue, concurrency, current_task_count, status, started_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			w.ID, string(w.Type), string(taskTypesJSON),
			w.Queue, w.Concurrency, w.CurrentTaskCount,
			string(w.Status), fmtDBTime(w.StartedAt),
		)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("CreateWorker: %w", err)
	}
	return w, nil
}

// GetWorker returns the worker with the given ID.
// Returns ErrNotFound if no such worker exists.
func (s *Store) GetWorker(ctx context.Context, id string) (*Worker, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, type, task_types, queue, concurrency, current_task_count,
		       status, started_at, heartbeat_at
		FROM workers
		WHERE id = ?`, id)
	w, err := scanWorker(row.Scan)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("GetWorker: %w", err)
	}
	return w, nil
}

// ListWorkers returns all registered workers ordered by started_at ASC.
func (s *Store) ListWorkers(ctx context.Context) ([]*Worker, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, type, task_types, queue, concurrency, current_task_count,
		       status, started_at, heartbeat_at
		FROM workers
		ORDER BY started_at ASC`)
	if err != nil {
		return nil, fmt.Errorf("ListWorkers: %w", err)
	}
	defer rows.Close()

	var workers []*Worker
	for rows.Next() {
		w, err := scanWorker(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("ListWorkers scan: %w", err)
		}
		workers = append(workers, w)
	}
	return workers, rows.Err()
}

// scanWorker reads one row from the workers table into a Worker.
func scanWorker(scan func(...any) error) (*Worker, error) {
	var (
		w            Worker
		workerType   string
		taskTypesRaw []byte
		status       string
		startedAt    string
		heartbeatAt  sql.NullString
	)
	if err := scan(
		&w.ID, &workerType, &taskTypesRaw,
		&w.Queue, &w.Concurrency, &w.CurrentTaskCount,
		&status, &startedAt, &heartbeatAt,
	); err != nil {
		return nil, err
	}

	w.Type = WorkerType(workerType)
	w.Status = WorkerStatus(status)

	if err := json.Unmarshal(taskTypesRaw, &w.TaskTypes); err != nil {
		return nil, fmt.Errorf("unmarshal task_types: %w", err)
	}

	var err error
	if w.StartedAt, err = parseDBTime(startedAt); err != nil {
		return nil, fmt.Errorf("started_at: %w", err)
	}
	if heartbeatAt.Valid {
		t, err := parseDBTime(heartbeatAt.String)
		if err != nil {
			return nil, fmt.Errorf("heartbeat_at: %w", err)
		}
		w.HeartbeatAt = &t
	}
	return &w, nil
}
