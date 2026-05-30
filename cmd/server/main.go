package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/peifengstudio/erminetq/internal/api"
	"github.com/peifengstudio/erminetq/internal/bridge"
	"github.com/peifengstudio/erminetq/internal/config"
	"github.com/peifengstudio/erminetq/internal/queue"
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
	cfg, dbPath, addr, err := parseServerFlags(args)
	if err != nil {
		return err
	}

	slog.Info("ErmineTQ starting",
		"version", Version,
		"db", dbPath,
		"addr", addr,
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

	// ── Task registry + Python Bridge ─────────────────────────────────────────
	registry := queue.NewRegistry()
	// TODO: register Go task handlers here, e.g.:
	//   registry.Register("send_email", handlers.SendEmail)

	if cfg.Bridge.Socket != "" {
		bridgeClient := bridge.NewClient(cfg.Bridge.Socket)
		defer bridgeClient.Close()
		registry.SetBridge(bridgeClient, cfg.Bridge.TaskTypes)
		slog.Info("bridge configured",
			"socket", cfg.Bridge.Socket,
			"types", cfg.Bridge.TaskTypes,
		)
	}

	// SSE broker: bridges store events to HTTP clients.
	broker := api.NewBroker()
	s.SetEventSink(broker)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	broker.Start(ctx)

	// HTTP handler + router.
	handler := api.NewHandler(s, broker)
	mux := http.NewServeMux()
	handler.Register(mux)

	srv := api.NewServer(addr, mux)
	if err := srv.Start(); err != nil {
		return fmt.Errorf("start http server: %w", err)
	}
	slog.Info("server running", "addr", addr)

	<-ctx.Done()
	slog.Info("shutting down...")

	// Wait for the broker fan-out goroutine to drain (ctx already cancelled above).
	broker.Wait()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Warn("http shutdown", "err", err)
	}
	return nil
}

// cmdMigrate applies pending migrations and exits.
// Useful for CI pre-flight checks or container init steps.
func cmdMigrate(args []string) error {
	cfg, dbPath, err := parseMigrateFlags(args)
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

// parseServerFlags parses flags for the "server" subcommand.
// Returns config, dbPath, httpAddr.
func parseServerFlags(args []string) (*config.Config, string, string, error) {
	fs := flag.NewFlagSet("server", flag.ExitOnError)

	configFlag := fs.String("config", "",
		fmt.Sprintf("path to TOML config file (env: %s, default: %s)",
			config.EnvConfig, config.DefaultConfigPath))
	dbFlag := fs.String("db", "",
		fmt.Sprintf("override database path — takes precedence over config and %s", config.EnvDB))
	addrFlag := fs.String("addr", ":8080", "HTTP listen address")

	if err := fs.Parse(args); err != nil {
		return nil, "", "", err
	}

	cfg, dbPath, err := loadConfig(*configFlag, *dbFlag)
	if err != nil {
		return nil, "", "", err
	}
	return cfg, dbPath, *addrFlag, nil
}

// parseMigrateFlags parses flags for the "migrate" subcommand.
func parseMigrateFlags(args []string) (*config.Config, string, error) {
	fs := flag.NewFlagSet("migrate", flag.ExitOnError)

	configFlag := fs.String("config", "",
		fmt.Sprintf("path to TOML config file (env: %s, default: %s)",
			config.EnvConfig, config.DefaultConfigPath))
	dbFlag := fs.String("db", "",
		fmt.Sprintf("override database path — takes precedence over config and %s", config.EnvDB))

	if err := fs.Parse(args); err != nil {
		return nil, "", err
	}
	return loadConfig(*configFlag, *dbFlag)
}

// loadConfig loads and returns the config, applying the -db flag override.
func loadConfig(configFlagVal, dbFlagVal string) (*config.Config, string, error) {
	configPath := config.ConfigFilePath(configFlagVal)
	cfg, err := config.Load(configPath)
	if err != nil {
		return nil, "", fmt.Errorf("load config %q: %w", configPath, err)
	}
	dbPath := cfg.DB.Path
	if dbFlagVal != "" {
		dbPath = dbFlagVal
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
