// Command server is a standalone ErmineTQ instance used only when you want to
// run the examples WITHOUT the main dev server.
//
// Normal workflow — use the real server:
//
//	make dev                              # Terminal 1: starts on :8080
//	make example-submit                   # Terminal 2: submit Go tasks
//	make example-bridge                   # Terminal 2: start Python Bridge
//	make example-py-submit                # Terminal 3: submit Python tasks
//
// Standalone workflow (no make dev):
//
//	go run ./examples/go/server           # Terminal 1: starts on :8081
//	go run ./examples/go/submit -addr http://localhost:8081
//
// The database is stored at examples/output/example.db.
// Task output files are written to examples/output/.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/peifengstudio/erminetq/examples/go/handlers"
	"github.com/peifengstudio/erminetq/internal/api"
	"github.com/peifengstudio/erminetq/internal/config"
	"github.com/peifengstudio/erminetq/internal/queue"
	"github.com/peifengstudio/erminetq/internal/scheduler"
	"github.com/peifengstudio/erminetq/internal/store"
)

const (
	addr   = ":8081"
	dbPath = "examples/output/example.db"
)

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run() error {
	slog.Info("ErmineTQ example server starting", "addr", addr, "db", dbPath)

	// ── Store ──────────────────────────────────────────────────────────────────
	s, err := store.Open(dbPath)
	if err != nil {
		return err
	}
	defer s.Close() //nolint:errcheck

	// ── Task registry: register all example Go handlers ───────────────────────
	reg := queue.NewRegistry()
	reg.Register("go_http_fetch", handlers.HTTPFetch)
	reg.Register("go_file_write", handlers.FileWrite)
	reg.Register("go_file_read", handlers.FileRead)
	reg.Register("go_ollama_chat", handlers.OllamaChat)
	slog.Info("registered task types", "types", reg.TaskTypes())

	// ── Minimal config (no per-queue limits for the example) ──────────────────
	cfg := &config.Config{
		Limits:    config.LimitsConfig{Global: 8},
		Queues:    map[string]config.QueueConfig{},
		TaskTypes: map[string]config.TaskTypeConfig{},
	}

	// ── SSE broker ────────────────────────────────────────────────────────────
	broker := api.NewBroker()
	s.SetEventSink(broker)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	broker.Start(ctx)
	defer broker.Wait()

	// ── Worker pool ───────────────────────────────────────────────────────────
	pool, err := queue.NewPool(ctx, queue.PoolConfig{
		Store:        s,
		Registry:     reg,
		Config:       cfg,
		Queue:        "default",
		Concurrency:  4,
		PollInterval: 300 * time.Millisecond,
		OnError: func(err error) {
			slog.Warn("pool error", "err", err)
		},
	})
	if err != nil {
		return err
	}
	pool.Start(ctx)
	defer pool.Wait()

	// ── Scheduler ─────────────────────────────────────────────────────────────
	sched := scheduler.New(s, scheduler.Config{
		OnError: func(err error) {
			slog.Warn("scheduler error", "err", err)
		},
	})
	sched.Start(ctx)
	defer sched.Wait()

	// ── HTTP API ──────────────────────────────────────────────────────────────
	handler := api.NewHandler(s, broker)
	mux := http.NewServeMux()
	handler.Register(mux)

	srv := api.NewServer(addr, mux)
	if err := srv.Start(); err != nil {
		return err
	}
	slog.Info("example server ready", "addr", addr)
	slog.Info("submit tasks with: go run ./examples/go/submit")

	<-ctx.Done()
	slog.Info("shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}
