package store

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// ErrNotFound is returned when a record cannot be found by its primary key.
var ErrNotFound = errors.New("not found")

// NewID returns a new random UUID string suitable for use as a primary key.
func NewID() string {
	return uuid.NewString()
}

// dbTimeLayout is the datetime format SQLite uses for CURRENT_TIMESTAMP values.
const dbTimeLayout = "2006-01-02 15:04:05"

// fmtDBTime formats t in UTC using the SQLite CURRENT_TIMESTAMP layout.
func fmtDBTime(t time.Time) string {
	return t.UTC().Format(dbTimeLayout)
}

// nullTime converts a *time.Time to an any value for use as a SQL parameter.
// A nil pointer maps to SQL NULL; a non-nil pointer is formatted with fmtDBTime.
func nullTime(t *time.Time) any {
	if t == nil {
		return nil
	}
	return fmtDBTime(*t)
}

// parseNullDBTime parses a sql.NullString DATETIME column into *time.Time.
// A NULL column (ns.Valid == false) returns nil without error.
func parseNullDBTime(ns sql.NullString) (*time.Time, error) {
	if !ns.Valid {
		return nil, nil
	}
	t, err := parseDBTime(ns.String)
	if err != nil {
		return nil, err
	}
	return &t, nil
}

// parseDBTime parses a DATETIME string stored by SQLite.
// It accepts the CURRENT_TIMESTAMP format ("2006-01-02 15:04:05") as primary
// and RFC3339/RFC3339Nano as fallbacks for values inserted as Go time.Time.
func parseDBTime(s string) (time.Time, error) {
	if t, err := time.ParseInLocation(dbTimeLayout, s, time.UTC); err == nil {
		return t, nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t.UTC(), nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t.UTC(), nil
	}
	return time.Time{}, fmt.Errorf("parseDBTime: unrecognized format %q", s)
}
