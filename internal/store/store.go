package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	_ "modernc.org/sqlite" // register the "sqlite" driver
)

// ErrStoreClosing is returned when a write is attempted on a closing Store.
var ErrStoreClosing = errors.New("store is closing")

// Store is the single source of truth for all task state in ErmineTQ.
//
// Architecture:
//   - All writes are serialized through a single goroutine (writeLoop) via
//     writeCh. This prevents concurrent-write contention on the SQLite WAL.
//   - Reads use the shared *sql.DB concurrently. SQLite WAL mode allows
//     multiple concurrent readers even while a writer is active.
//   - Every state transition (tasks, attempts, workers, task_events) must
//     go through Store methods in a single transaction. Never write these
//     tables directly from handlers or workers.
type Store struct {
	db        *sql.DB
	writeCh   chan writeOp
	closeCh   chan struct{} // closed once by closeOnce to signal shutdown
	done      chan struct{} // closed by writeLoop when it exits
	closeOnce sync.Once
}

// writeOp is a unit of work dispatched to the single write goroutine.
type writeOp struct {
	fn  func(*sql.Tx) error
	res chan<- error
}

// Open opens (or creates) the SQLite database at path, applies any pending
// migrations, and starts the background write goroutine.
//
// The DSN pragmas set WAL journal mode, foreign-key enforcement, and a
// 5-second busy timeout to absorb momentary lock contention.
func Open(path string) (*Store, error) {
	dsn := path +
		"?_journal_mode=WAL" +
		"&_foreign_keys=on" +
		"&_busy_timeout=5000" +
		"&_synchronous=NORMAL"

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

	// Limit to a single connection. SQLite WAL allows concurrent readers
	// through SQLite's own locking, but the Go pool serialises Go-level
	// callers. For v0.1 correctness this is sufficient; a separate read pool
	// can be added when benchmark data justifies the complexity.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping db: %w", err)
	}

	if err := Migrate(db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}

	s := &Store{
		db:      db,
		writeCh: make(chan writeOp, 64),
		closeCh: make(chan struct{}),
		done:    make(chan struct{}),
	}
	go s.writeLoop()
	return s, nil
}

// Close signals the write goroutine to stop, drains any pending writes with
// ErrStoreClosing, waits for the goroutine to exit, then closes the database.
// Safe to call multiple times; only the first call has effect.
func (s *Store) Close() error {
	s.closeOnce.Do(func() { close(s.closeCh) })
	<-s.done
	return s.db.Close()
}

// write dispatches fn to the single write goroutine and blocks until the
// transaction commits or rolls back. It respects ctx cancellation both while
// waiting for the write goroutine to accept the op and while waiting for the
// result.
func (s *Store) write(ctx context.Context, fn func(*sql.Tx) error) error {
	res := make(chan error, 1)
	op := writeOp{fn: fn, res: res}

	select {
	case s.writeCh <- op:
	case <-ctx.Done():
		return ctx.Err()
	case <-s.closeCh:
		return ErrStoreClosing
	}

	select {
	case err := <-res:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// writeLoop is the single goroutine that serialises all SQLite writes.
// It exits when closeCh is closed, draining any buffered ops first.
func (s *Store) writeLoop() {
	defer close(s.done)
	for {
		select {
		case op := <-s.writeCh:
			op.res <- s.execTx(op.fn)
		case <-s.closeCh:
			// Drain buffered ops without executing them
			for {
				select {
				case op := <-s.writeCh:
					op.res <- ErrStoreClosing
				default:
					return
				}
			}
		}
	}
}

// execTx runs fn inside a new transaction, rolling back on error.
func (s *Store) execTx(fn func(*sql.Tx) error) error {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}
