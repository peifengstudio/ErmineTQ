package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/peifengstudio/erminetq/internal/config"
	"github.com/peifengstudio/erminetq/internal/store"
)

// Build-time variables injected via -ldflags (see Makefile).
var (
	Version   = "dev"
	Commit    = "none"
	BuildTime = "unknown"
)

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
	cfg, dbPath, err := parseFlags("server", args)
	if err != nil {
		return err
	}

	slog.Info("ErmineTQ starting",
		"version", Version,
		"db", dbPath,
		"global_limit", cfg.Limits.Global,
	)

	s, err := store.Open(dbPath)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			slog.Warn("store close", "err", err)
		}
	}()

	slog.Info("database ready", "db", dbPath)

	// TODO: wire up internal/queue, internal/api, internal/scheduler, internal/dashboard

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	slog.Info("server running — press Ctrl+C to stop")
	<-ctx.Done()
	slog.Info("shutting down...")
	return nil
}

// cmdMigrate applies pending migrations and exits.
// Useful for CI pre-flight checks or container init steps.
func cmdMigrate(args []string) error {
	cfg, dbPath, err := parseFlags("migrate", args)
	if err != nil {
		return err
	}
	_ = cfg // config will be used by later subsystems

	slog.Info("applying migrations", "db", dbPath)

	s, err := store.Open(dbPath)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	if err := s.Close(); err != nil {
		return fmt.Errorf("close store: %w", err)
	}

	slog.Info("migrations complete", "db", dbPath)
	return nil
}

// parseFlags is shared by cmdServer and cmdMigrate.
// It loads the config file, then applies the -db override if provided.
//
// DB path resolution order (highest priority first):
//  1. -db flag (explicit override)
//  2. ERMINETQ_DB environment variable
//  3. db.path in the config file
//  4. "erminetq.db" (compiled-in default)
func parseFlags(cmd string, args []string) (*config.Config, string, error) {
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)

	configFlag := fs.String("config", "",
		fmt.Sprintf("path to TOML config file (env: %s, default: %s)",
			config.EnvConfig, config.DefaultConfigPath))

	dbFlag := fs.String("db", "",
		fmt.Sprintf("override database path — takes precedence over config and %s", config.EnvDB))

	if err := fs.Parse(args); err != nil {
		return nil, "", err
	}

	configPath := config.ConfigFilePath(*configFlag)

	cfg, err := config.Load(configPath)
	if err != nil {
		return nil, "", fmt.Errorf("load config %q: %w", configPath, err)
	}

	// -db flag is the final override.
	dbPath := cfg.DB.Path
	if *dbFlag != "" {
		dbPath = *dbFlag
	}

	return cfg, dbPath, nil
}

func usage() {
	fmt.Fprintf(os.Stderr, `ErmineTQ %s — task queue for local Python services

Usage:
  erminetq <command> [flags]

Commands:
  server    Start the ErmineTQ server (also applies migrations on startup)
  migrate   Apply pending database migrations and exit
  version   Print version information and exit

Flags (server / migrate):
  -config string
        Path to TOML config file.
        Env: %s  (default: %s)

  -db string
        Override database path. Takes precedence over config file and %s.

`, Version, config.EnvConfig, config.DefaultConfigPath, config.EnvDB)
}
