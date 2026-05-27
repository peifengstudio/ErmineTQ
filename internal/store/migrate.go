package store

import (
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Migrate ensures the schema_migrations tracking table exists, then applies
// every unapplied migration in version order. It is safe to call multiple
// times (idempotent).
func Migrate(db *sql.DB) error {
	if err := ensureMigrationsTable(db); err != nil {
		return err
	}

	applied, err := appliedVersions(db)
	if err != nil {
		return err
	}

	files, err := fs.Glob(migrationsFS, "migrations/*.sql")
	if err != nil {
		return fmt.Errorf("list migration files: %w", err)
	}
	sort.Strings(files) // lexicographic order → 001 before 002

	for _, file := range files {
		version, desc, err := parseMigrationName(file)
		if err != nil {
			return err
		}
		if applied[version] {
			continue
		}

		content, err := migrationsFS.ReadFile(file)
		if err != nil {
			return fmt.Errorf("read %s: %w", file, err)
		}

		if err := applyMigration(db, version, desc, string(content)); err != nil {
			return fmt.Errorf("apply %s: %w", file, err)
		}
	}
	return nil
}

// ensureMigrationsTable creates the schema_migrations table if it does not
// exist. Uses IF NOT EXISTS so it is safe on repeated calls.
func ensureMigrationsTable(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version     INTEGER  PRIMARY KEY,
			description TEXT     NOT NULL,
			applied_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`)
	if err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}
	return nil
}

// appliedVersions returns the set of migration version numbers already recorded
// in schema_migrations.
func appliedVersions(db *sql.DB) (map[int]bool, error) {
	rows, err := db.Query(`SELECT version FROM schema_migrations`)
	if err != nil {
		return nil, fmt.Errorf("query schema_migrations: %w", err)
	}
	defer rows.Close()

	applied := make(map[int]bool)
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			return nil, fmt.Errorf("scan schema_migrations row: %w", err)
		}
		applied[v] = true
	}
	return applied, rows.Err()
}

// applyMigration runs all SQL statements in content inside a single
// transaction, then records the migration in schema_migrations.
func applyMigration(db *sql.DB, version int, desc, content string) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	for _, stmt := range splitSQL(content) {
		if _, err := tx.Exec(stmt); err != nil {
			_ = tx.Rollback()
			// Include the first 80 chars of the failing statement for context.
			preview := stmt
			if len(preview) > 80 {
				preview = preview[:80] + "…"
			}
			return fmt.Errorf("exec %q: %w", preview, err)
		}
	}

	if _, err := tx.Exec(
		`INSERT INTO schema_migrations (version, description) VALUES (?, ?)`,
		version, desc,
	); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("record migration v%d: %w", version, err)
	}

	return tx.Commit()
}

// parseMigrationName extracts (version, description) from a path whose
// basename matches "NNN_description.sql", e.g. "migrations/001_initial.sql"
// → (1, "initial").
func parseMigrationName(path string) (int, string, error) {
	base := path[strings.LastIndex(path, "/")+1:]
	base = strings.TrimSuffix(base, ".sql")

	parts := strings.SplitN(base, "_", 2)
	if len(parts) != 2 {
		return 0, "", fmt.Errorf("migration filename must be NNN_description.sql, got: %s", path)
	}

	version, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, "", fmt.Errorf("non-numeric version prefix in %s: %w", path, err)
	}
	return version, parts[1], nil
}

// splitSQL splits a SQL script into individual statements by splitting on ";"
// at end-of-line (after stripping comments). This simple approach is safe for
// DDL-only migration files that do not embed semicolons inside string literals.
func splitSQL(script string) []string {
	var stmts []string
	var buf strings.Builder

	for _, line := range strings.Split(script, "\n") {
		// Strip inline -- comments
		if idx := strings.Index(line, "--"); idx >= 0 {
			line = line[:idx]
		}
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		buf.WriteString(trimmed)
		buf.WriteByte('\n')

		if strings.HasSuffix(trimmed, ";") {
			stmt := strings.TrimSpace(buf.String())
			if stmt != "" && stmt != ";" {
				stmts = append(stmts, stmt)
			}
			buf.Reset()
		}
	}

	// Catch any trailing statement without a final semicolon
	if stmt := strings.TrimSpace(buf.String()); stmt != "" {
		stmts = append(stmts, stmt)
	}
	return stmts
}
