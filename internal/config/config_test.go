package config_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/peifengstudio/erminetq/internal/config"
)

// writeConfig writes content to a temp file and returns its path.
func writeConfig(t *testing.T, content string) string {
	t.Helper()
	f := filepath.Join(t.TempDir(), "erminetq.toml")
	if err := os.WriteFile(f, []byte(content), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return f
}

// ── Load: file-not-found ──────────────────────────────────────────────────────

func TestLoad_NoFile_UsesDefaults(t *testing.T) {
	cfg, err := config.Load("/nonexistent/path/erminetq.toml")
	if err != nil {
		t.Fatalf("Load with no file should not error, got: %v", err)
	}
	if cfg.Limits.Global != config.DefaultGlobalLimit {
		t.Errorf("global limit: want %d, got %d", config.DefaultGlobalLimit, cfg.Limits.Global)
	}
	if cfg.DB.Path == "" {
		t.Error("DB.Path should not be empty when file is absent")
	}
	if len(cfg.Queues) != 0 {
		t.Errorf("Queues should be empty by default, got %d entries", len(cfg.Queues))
	}
	if len(cfg.TaskTypes) != 0 {
		t.Errorf("TaskTypes should be empty by default, got %d entries", len(cfg.TaskTypes))
	}
}

// ── Load: full config ─────────────────────────────────────────────────────────

func TestLoad_FullConfig(t *testing.T) {
	path := writeConfig(t, `
[db]
path = "/data/erminetq.db"

[limits]
global = 64

[queues.default]
limit = 16

[queues.ollama]
limit = 2

[task_types.ollama_generate]
queue = "ollama"
limit = 1

[task_types.fetch_api]
queue = "http"
limit = 50
`)
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.DB.Path != "/data/erminetq.db" {
		t.Errorf("DB.Path: want /data/erminetq.db, got %q", cfg.DB.Path)
	}
	if cfg.Limits.Global != 64 {
		t.Errorf("global limit: want 64, got %d", cfg.Limits.Global)
	}
	if cfg.Queues["default"].Limit != 16 {
		t.Errorf("queues.default.limit: want 16, got %d", cfg.Queues["default"].Limit)
	}
	if cfg.Queues["ollama"].Limit != 2 {
		t.Errorf("queues.ollama.limit: want 2, got %d", cfg.Queues["ollama"].Limit)
	}
	if cfg.TaskTypes["ollama_generate"].Limit != 1 {
		t.Errorf("task_types.ollama_generate.limit: want 1, got %d", cfg.TaskTypes["ollama_generate"].Limit)
	}
	if cfg.TaskTypes["ollama_generate"].Queue != "ollama" {
		t.Errorf("task_types.ollama_generate.queue: want ollama, got %q", cfg.TaskTypes["ollama_generate"].Queue)
	}
	if cfg.TaskTypes["fetch_api"].Limit != 50 {
		t.Errorf("task_types.fetch_api.limit: want 50, got %d", cfg.TaskTypes["fetch_api"].Limit)
	}
}

// ── Load: partial config inherits defaults ────────────────────────────────────

func TestLoad_PartialConfig_InheritsDefaults(t *testing.T) {
	path := writeConfig(t, `
[queues.ollama]
limit = 2
`)
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	// global limit not set in file → should use default
	if cfg.Limits.Global != config.DefaultGlobalLimit {
		t.Errorf("global limit: want %d, got %d", config.DefaultGlobalLimit, cfg.Limits.Global)
	}
}

// ── Load: env var overrides db.path ──────────────────────────────────────────

func TestLoad_EnvDB_OverridesFilePath(t *testing.T) {
	t.Setenv(config.EnvDB, "/env/override.db")

	path := writeConfig(t, `
[db]
path = "/file/path.db"
`)
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.DB.Path != "/env/override.db" {
		t.Errorf("ERMINETQ_DB should override file: want /env/override.db, got %q", cfg.DB.Path)
	}
}

func TestLoad_EnvDB_AppliedWhenNoFile(t *testing.T) {
	t.Setenv(config.EnvDB, "/env/mydb.db")

	cfg, err := config.Load("/nonexistent/erminetq.toml")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.DB.Path != "/env/mydb.db" {
		t.Errorf("ERMINETQ_DB should set DB.Path: want /env/mydb.db, got %q", cfg.DB.Path)
	}
}

// ── Load: malformed TOML ──────────────────────────────────────────────────────

func TestLoad_MalformedTOML_ReturnsError(t *testing.T) {
	path := writeConfig(t, `
[limits
global = not_a_number
`)
	_, err := config.Load(path)
	if err == nil {
		t.Fatal("expected error for malformed TOML, got nil")
	}
}

// ── Validation ────────────────────────────────────────────────────────────────

func TestLoad_Validation_GlobalBelowOne(t *testing.T) {
	for _, v := range []int{0, -1, -100} {
		path := writeConfig(t, fmt.Sprintf(`
[limits]
global = %d
`, v))
		_, err := config.Load(path)
		if err == nil {
			t.Errorf("global = %d: expected validation error, got nil", v)
		}
	}
}

func TestLoad_Validation_NegativeQueueLimit(t *testing.T) {
	path := writeConfig(t, `
[queues.http]
limit = -1
`)
	_, err := config.Load(path)
	if err == nil {
		t.Fatal("expected validation error for negative queue limit, got nil")
	}
}

func TestLoad_Validation_NegativeTaskTypeLimit(t *testing.T) {
	path := writeConfig(t, `
[task_types.fetch_api]
limit = -5
`)
	_, err := config.Load(path)
	if err == nil {
		t.Fatal("expected validation error for negative task type limit, got nil")
	}
}

func TestLoad_Validation_ZeroQueueLimit_IsAllowed(t *testing.T) {
	path := writeConfig(t, `
[queues.default]
limit = 0
`)
	_, err := config.Load(path)
	if err != nil {
		t.Fatalf("limit = 0 (unlimited) should be valid, got: %v", err)
	}
}

// ── Accessor helpers ──────────────────────────────────────────────────────────

func TestQueueLimit_ConfiguredQueue(t *testing.T) {
	path := writeConfig(t, `
[queues.ollama]
limit = 2
`)
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := cfg.QueueLimit("ollama"); got != 2 {
		t.Errorf("QueueLimit(ollama): want 2, got %d", got)
	}
}

func TestQueueLimit_UnknownQueue_ReturnsZero(t *testing.T) {
	cfg, _ := config.Load("/nonexistent/erminetq.toml")
	if got := cfg.QueueLimit("not_configured"); got != 0 {
		t.Errorf("QueueLimit(unknown): want 0, got %d", got)
	}
}

func TestTaskTypeLimit_ConfiguredType(t *testing.T) {
	path := writeConfig(t, `
[task_types.ollama_generate]
queue = "ollama"
limit = 1
`)
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := cfg.TaskTypeLimit("ollama_generate"); got != 1 {
		t.Errorf("TaskTypeLimit(ollama_generate): want 1, got %d", got)
	}
}

func TestTaskTypeLimit_UnknownType_ReturnsZero(t *testing.T) {
	cfg, _ := config.Load("/nonexistent/erminetq.toml")
	if got := cfg.TaskTypeLimit("not_configured"); got != 0 {
		t.Errorf("TaskTypeLimit(unknown): want 0, got %d", got)
	}
}

// ── ConfigFilePath helper ─────────────────────────────────────────────────────

func TestConfigFilePath_FlagTakesPrecedence(t *testing.T) {
	t.Setenv(config.EnvConfig, "/env/erminetq.toml")
	got := config.ConfigFilePath("/flag/erminetq.toml")
	if got != "/flag/erminetq.toml" {
		t.Errorf("flag should win: want /flag/erminetq.toml, got %q", got)
	}
}

func TestConfigFilePath_EnvFallback(t *testing.T) {
	t.Setenv(config.EnvConfig, "/env/erminetq.toml")
	got := config.ConfigFilePath("")
	if got != "/env/erminetq.toml" {
		t.Errorf("env should be used when flag is empty: want /env/erminetq.toml, got %q", got)
	}
}

func TestConfigFilePath_DefaultFallback(t *testing.T) {
	t.Setenv(config.EnvConfig, "") // ensure env is clear
	got := config.ConfigFilePath("")
	if got != config.DefaultConfigPath {
		t.Errorf("default fallback: want %q, got %q", config.DefaultConfigPath, got)
	}
}

// ── [bridge] section ──────────────────────────────────────────────────────────

func TestLoad_BridgeConfig(t *testing.T) {
	path := writeConfig(t, `
[bridge]
socket = "/tmp/erminetq_bridge.sock"
task_types = ["analyze_data", "train_model"]
`)
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Bridge.Socket != "/tmp/erminetq_bridge.sock" {
		t.Errorf("Bridge.Socket = %q, want /tmp/erminetq_bridge.sock", cfg.Bridge.Socket)
	}
	if len(cfg.Bridge.TaskTypes) != 2 {
		t.Fatalf("Bridge.TaskTypes len=%d, want 2", len(cfg.Bridge.TaskTypes))
	}
	if cfg.Bridge.TaskTypes[0] != "analyze_data" || cfg.Bridge.TaskTypes[1] != "train_model" {
		t.Errorf("Bridge.TaskTypes = %v", cfg.Bridge.TaskTypes)
	}
}

func TestLoad_BridgeConfig_Empty(t *testing.T) {
	// No [bridge] section — bridge is disabled; should not error.
	cfg, err := config.Load("/nonexistent/erminetq.toml")
	if err != nil {
		t.Fatalf("Load with no file: %v", err)
	}
	if cfg.Bridge.Socket != "" {
		t.Errorf("Bridge.Socket should default to empty, got %q", cfg.Bridge.Socket)
	}
	if len(cfg.Bridge.TaskTypes) != 0 {
		t.Errorf("Bridge.TaskTypes should default to empty, got %v", cfg.Bridge.TaskTypes)
	}
}

func TestLoad_BridgeConfig_SocketOnly(t *testing.T) {
	// socket set but no task_types — valid config, bridge is enabled but
	// won't route anything (operator can add task_types later).
	path := writeConfig(t, `
[bridge]
socket = "/var/run/bridge.sock"
`)
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Bridge.Socket != "/var/run/bridge.sock" {
		t.Errorf("Bridge.Socket = %q", cfg.Bridge.Socket)
	}
	if len(cfg.Bridge.TaskTypes) != 0 {
		t.Errorf("Bridge.TaskTypes should be empty, got %v", cfg.Bridge.TaskTypes)
	}
}
