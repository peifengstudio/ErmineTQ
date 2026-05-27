package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/peifengstudio/erminetq/internal/store"
)

// Build-time variables injected by -ldflags (see Makefile).
var (
	Version   = "dev"
	Commit    = "none"
	BuildTime = "unknown"
)

// envDB is the environment variable that sets the default database path.
const envDB = "ERMINETQ_DB"

// defaultDB returns the value of ERMINETQ_DB, falling back to "erminetq.db".
func defaultDB() string {
	if v := os.Getenv(envDB); v != "" {
		return v
	}
	return "erminetq.db"
}

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) < 2 {
		usage()
		return fmt.Errorf("subcommand required")
	}

	switch os.Args[1] {
	case "server":
		return cmdServer(os.Args[2:])
	case "migrate":
		return cmdMigrate(os.Args[2:])
	case "version":
		fmt.Printf("erminetq %s (commit %s, built %s)\n", Version, Commit, BuildTime)
		return nil
	case "help", "-h", "--help":
		usage()
		return nil
	default:
		usage()
		return fmt.Errorf("unknown subcommand: %q", os.Args[1])
	}
}

// cmdServer opens the store, applies migrations, then blocks until SIGINT/SIGTERM.
func cmdServer(args []string) error {
	fs := flag.NewFlagSet("server", flag.ExitOnError)
	dbPath := fs.String("db", defaultDB(), fmt.Sprintf(
		"path to SQLite database file (env: %s)", envDB,
	))
	if err := fs.Parse(args); err != nil {
		return err
	}

	slog.Info("ErmineTQ starting", "version", Version, "db", *dbPath)

	s, err := store.Open(*dbPath)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			slog.Warn("store close", "err", err)
		}
	}()

	slog.Info("database ready", "db", *dbPath)

	// TODO: wire up internal/queue, internal/api, internal/scheduler, internal/dashboard

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	slog.Info("server running — press Ctrl+C to stop")
	<-ctx.Done()
	slog.Info("shutting down...")
	return nil
}

// cmdMigrate applies pending migrations and exits.
// Useful for CI pre-flight checks or container init containers.
func cmdMigrate(args []string) error {
	fs := flag.NewFlagSet("migrate", flag.ExitOnError)
	dbPath := fs.String("db", defaultDB(), fmt.Sprintf(
		"path to SQLite database file (env: %s)", envDB,
	))
	if err := fs.Parse(args); err != nil {
		return err
	}

	slog.Info("applying migrations", "db", *dbPath)

	s, err := store.Open(*dbPath)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	if err := s.Close(); err != nil {
		return fmt.Errorf("close store: %w", err)
	}

	slog.Info("migrations complete", "db", *dbPath)
	return nil
}

func usage() {
	fmt.Fprintf(os.Stderr, `ErmineTQ %s — task queue for local Python services

Usage:
  erminetq <command> [flags]

Commands:
  server    Start the ErmineTQ server (also applies migrations on startup)
  migrate   Apply pending database migrations and exit
  version   Print version and exit

Flags (server / migrate):
  -db string
        Path to SQLite database file.
        Env: %s  (default: erminetq.db)

`, Version, envDB)
}
