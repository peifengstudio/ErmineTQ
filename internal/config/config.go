package config

import (
	"errors"
	"fmt"
	"os"

	"github.com/BurntSushi/toml"
)

// Environment variables recognised by ErmineTQ.
const (
	// EnvDB overrides [db] path in the config file.
	EnvDB = "ERMINETQ_DB"
	// EnvConfig overrides the default config file path used by the CLI.
	EnvConfig = "ERMINETQ_CONFIG"
)

// DefaultConfigPath is the config file name looked up in the working directory
// when -config is not passed.
const DefaultConfigPath = "erminetq.toml"

// DefaultGlobalLimit is the global concurrency cap applied when the
// config file does not specify [limits] global.
const DefaultGlobalLimit = 128

// ── Top-level ─────────────────────────────────────────────────────────────────

// Config is the root configuration for an ErmineTQ instance.
type Config struct {
	DB        DBConfig                  `toml:"db"`
	Limits    LimitsConfig              `toml:"limits"`
	Queues    map[string]QueueConfig    `toml:"queues"`
	TaskTypes map[string]TaskTypeConfig `toml:"task_types"`
}

// ── Sub-sections ──────────────────────────────────────────────────────────────

// DBConfig controls where the SQLite database is stored.
type DBConfig struct {
	// Path is the file-system path to the SQLite database.
	// Override precedence (highest first):
	//   1. -db CLI flag
	//   2. ERMINETQ_DB environment variable
	//   3. this field in the TOML file
	//   4. "erminetq.db" (compiled-in default)
	Path string `toml:"path"`
}

// LimitsConfig defines the global execution limit.
type LimitsConfig struct {
	// Global is the maximum number of tasks that may run concurrently
	// across all queues. Must be >= 1. Default: 128.
	Global int `toml:"global"`
}

// QueueConfig defines the execution limit for one named queue.
type QueueConfig struct {
	// Limit is the maximum number of tasks in this queue that may run
	// concurrently. 0 means unlimited (only the global limit applies).
	Limit int `toml:"limit"`
}

// TaskTypeConfig defines the execution limit for one task type.
type TaskTypeConfig struct {
	// Queue identifies which queue this task type is routed to.
	// Informational only; used for documentation and future validation.
	Queue string `toml:"queue"`

	// Limit is the maximum number of tasks of this type that may run
	// concurrently. 0 means unlimited (queue and global limits still apply).
	Limit int `toml:"limit"`
}

// ── Loading ───────────────────────────────────────────────────────────────────

// Load reads the TOML config file at path.
//
//   - If the file does not exist, compiled-in defaults are returned (not an error).
//   - If the file exists but is malformed, an error is returned.
//   - ERMINETQ_DB always overrides [db] path, regardless of the file.
//   - After loading, all values are validated; invalid configs return an error.
func Load(path string) (*Config, error) {
	cfg := defaults()

	switch _, err := os.Stat(path); {
	case errors.Is(err, os.ErrNotExist):
		// Config file is optional; proceed with defaults.
	case err != nil:
		return nil, fmt.Errorf("stat config file %q: %w", path, err)
	default:
		// File exists — decode it, overwriting defaults where keys are present.
		if _, err := toml.DecodeFile(path, cfg); err != nil {
			return nil, fmt.Errorf("parse config file %q: %w", path, err)
		}
	}

	// ERMINETQ_DB always wins over the config file value.
	if v := os.Getenv(EnvDB); v != "" {
		cfg.DB.Path = v
	}

	// db.path: only fall back to the hardcoded default when still empty
	// (neither the file nor ERMINETQ_DB provided a value).
	if cfg.DB.Path == "" {
		cfg.DB.Path = "erminetq.db"
	}

	// NOTE: limits.global is intentionally NOT defaulted here.
	// defaults() already sets it to DefaultGlobalLimit. If the TOML file
	// explicitly wrote global = 0, we want validation to reject it, not
	// silently replace it with the default.

	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// ConfigFilePath returns the effective config file path, checking
// ERMINETQ_CONFIG and then falling back to DefaultConfigPath.
// Pass the value of the -config CLI flag; empty string means "not set by user".
func ConfigFilePath(flag string) string {
	if flag != "" {
		return flag
	}
	if v := os.Getenv(EnvConfig); v != "" {
		return v
	}
	return DefaultConfigPath
}

// ── Convenience accessors ─────────────────────────────────────────────────────

// QueueLimit returns the configured concurrency limit for queue.
// Returns 0 if the queue is not listed (unlimited at queue scope).
func (c *Config) QueueLimit(queue string) int {
	if q, ok := c.Queues[queue]; ok {
		return q.Limit
	}
	return 0
}

// TaskTypeLimit returns the configured concurrency limit for taskType.
// Returns 0 if the type is not listed (unlimited at task-type scope).
func (c *Config) TaskTypeLimit(taskType string) int {
	if t, ok := c.TaskTypes[taskType]; ok {
		return t.Limit
	}
	return 0
}

// ── Internal ──────────────────────────────────────────────────────────────────

func defaults() *Config {
	return &Config{
		DB:        DBConfig{},
		Limits:    LimitsConfig{Global: DefaultGlobalLimit},
		Queues:    make(map[string]QueueConfig),
		TaskTypes: make(map[string]TaskTypeConfig),
	}
}

func (c *Config) validate() error {
	if c.Limits.Global < 1 {
		return fmt.Errorf("limits.global must be >= 1, got %d", c.Limits.Global)
	}
	for name, q := range c.Queues {
		if q.Limit < 0 {
			return fmt.Errorf("queues.%s.limit must be >= 0, got %d", name, q.Limit)
		}
	}
	for name, t := range c.TaskTypes {
		if t.Limit < 0 {
			return fmt.Errorf("task_types.%s.limit must be >= 0, got %d", name, t.Limit)
		}
	}
	return nil
}
