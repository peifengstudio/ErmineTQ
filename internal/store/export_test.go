// export_test.go exposes Store internals to tests in package store_test.
// Do not import or use these symbols in non-test code.
package store

import "database/sql"

// DB returns the underlying *sql.DB for test fixture setup only.
func (s *Store) DB() *sql.DB { return s.db }
