package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// CreateScheduleInput holds caller-supplied fields for a new schedule.
// Exactly one of CronExpr or IntervalSecs should be set for a valid schedule,
// but this is enforced by the caller, not by CreateSchedule.
type CreateScheduleInput struct {
	TaskType     string
	Queue        string          // "" → "default"
	Payload      json.RawMessage // nil → NULL
	CronExpr     *string
	IntervalSecs *int
	Enabled      bool
	NextRunAt    *time.Time
}

// CreateSchedule inserts a new schedule.
func (s *Store) CreateSchedule(ctx context.Context, in CreateScheduleInput) (*Schedule, error) {
	sc := &Schedule{
		ID:           NewID(),
		TaskType:     in.TaskType,
		Queue:        in.Queue,
		Payload:      in.Payload,
		CronExpr:     in.CronExpr,
		IntervalSecs: in.IntervalSecs,
		Enabled:      in.Enabled,
		LastRunAt:    nil,
		NextRunAt:    in.NextRunAt,
		CreatedAt:    time.Now().UTC(),
	}
	if sc.Queue == "" {
		sc.Queue = "default"
	}

	err := s.write(ctx, func(tx *sql.Tx) error {
		_, err := tx.Exec(`
			INSERT INTO schedules
				(id, task_type, queue, payload, cron_expr, interval_secs,
				 enabled, next_run_at, created_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			sc.ID, sc.TaskType, sc.Queue,
			[]byte(sc.Payload), sc.CronExpr, sc.IntervalSecs,
			sc.Enabled, nullTime(sc.NextRunAt), fmtDBTime(sc.CreatedAt),
		)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("CreateSchedule: %w", err)
	}
	return sc, nil
}

// GetSchedule returns the schedule with the given ID.
// Returns ErrNotFound if no such schedule exists.
func (s *Store) GetSchedule(ctx context.Context, id string) (*Schedule, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, task_type, queue, payload, cron_expr, interval_secs,
		       enabled, last_run_at, next_run_at, created_at
		FROM schedules
		WHERE id = ?`, id)
	sc, err := scanSchedule(row.Scan)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("GetSchedule: %w", err)
	}
	return sc, nil
}

// ListSchedules returns all schedules ordered by created_at ASC.
func (s *Store) ListSchedules(ctx context.Context) ([]*Schedule, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, task_type, queue, payload, cron_expr, interval_secs,
		       enabled, last_run_at, next_run_at, created_at
		FROM schedules
		ORDER BY created_at ASC`)
	if err != nil {
		return nil, fmt.Errorf("ListSchedules: %w", err)
	}
	defer rows.Close()

	var schedules []*Schedule
	for rows.Next() {
		sc, err := scanSchedule(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("ListSchedules scan: %w", err)
		}
		schedules = append(schedules, sc)
	}
	return schedules, rows.Err()
}

// UpdateScheduleInput holds the fields that can be patched on a schedule.
// A nil pointer field is left unchanged.
type UpdateScheduleInput struct {
	Enabled      *bool
	CronExpr     *string
	IntervalSecs *int
	NextRunAt    *time.Time
}

// UpdateSchedule applies a partial update to the schedule with the given ID
// and returns the updated schedule.
// Returns ErrNotFound if no such schedule exists.
// If no fields are set in the input, the current schedule is returned unchanged.
func (s *Store) UpdateSchedule(ctx context.Context, id string, in UpdateScheduleInput) (*Schedule, error) {
	sets := make([]string, 0, 4)
	args := make([]any, 0, 5)

	if in.Enabled != nil {
		sets = append(sets, "enabled = ?")
		args = append(args, *in.Enabled)
	}
	if in.CronExpr != nil {
		sets = append(sets, "cron_expr = ?")
		args = append(args, *in.CronExpr)
	}
	if in.IntervalSecs != nil {
		sets = append(sets, "interval_secs = ?")
		args = append(args, *in.IntervalSecs)
	}
	if in.NextRunAt != nil {
		sets = append(sets, "next_run_at = ?")
		args = append(args, fmtDBTime(*in.NextRunAt))
	}

	if len(sets) == 0 {
		return s.GetSchedule(ctx, id)
	}

	query := "UPDATE schedules SET " + strings.Join(sets, ", ") + " WHERE id = ?"
	args = append(args, id)

	var affected int64
	err := s.write(ctx, func(tx *sql.Tx) error {
		result, err := tx.Exec(query, args...)
		if err != nil {
			return err
		}
		affected, err = result.RowsAffected()
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("UpdateSchedule: %w", err)
	}
	if affected == 0 {
		return nil, ErrNotFound
	}
	return s.GetSchedule(ctx, id)
}

// scanSchedule reads one row from the schedules table into a Schedule.
func scanSchedule(scan func(...any) error) (*Schedule, error) {
	var (
		sc           Schedule
		payload      []byte
		cronExpr     sql.NullString
		intervalSecs sql.NullInt64
		enabled      bool
		lastRunAt    sql.NullString
		nextRunAt    sql.NullString
		createdAt    string
	)
	if err := scan(
		&sc.ID, &sc.TaskType, &sc.Queue,
		&payload, &cronExpr, &intervalSecs,
		&enabled, &lastRunAt, &nextRunAt, &createdAt,
	); err != nil {
		return nil, err
	}

	sc.Payload = json.RawMessage(payload)
	sc.Enabled = enabled

	if cronExpr.Valid {
		sc.CronExpr = &cronExpr.String
	}
	if intervalSecs.Valid {
		v := int(intervalSecs.Int64)
		sc.IntervalSecs = &v
	}
	if lastRunAt.Valid {
		t, err := parseDBTime(lastRunAt.String)
		if err != nil {
			return nil, fmt.Errorf("last_run_at: %w", err)
		}
		sc.LastRunAt = &t
	}
	if nextRunAt.Valid {
		t, err := parseDBTime(nextRunAt.String)
		if err != nil {
			return nil, fmt.Errorf("next_run_at: %w", err)
		}
		sc.NextRunAt = &t
	}

	var err error
	if sc.CreatedAt, err = parseDBTime(createdAt); err != nil {
		return nil, fmt.Errorf("created_at: %w", err)
	}
	return &sc, nil
}
