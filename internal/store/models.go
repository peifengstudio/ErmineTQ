package store

import (
	"encoding/json"
	"time"
)

// ── Task ─────────────────────────────────────────────────────────────────────

// TaskStatus is the lifecycle state of a task.
// Note: "failed" is NOT a valid task status — it exists only on Attempt.
type TaskStatus string

const (
	TaskStatusQueued     TaskStatus = "queued"
	TaskStatusRunning    TaskStatus = "running"
	TaskStatusRetrying   TaskStatus = "retrying"
	TaskStatusSucceeded  TaskStatus = "succeeded"
	TaskStatusDead       TaskStatus = "dead"
	TaskStatusHalted     TaskStatus = "halted"
	TaskStatusCancelled  TaskStatus = "cancelled"
	TaskStatusSuperseded TaskStatus = "superseded"
)

// Task is the central scheduling unit.
type Task struct {
	ID          string
	Type        string
	Queue       string
	Payload     json.RawMessage // arbitrary JSON, passed to the worker as-is
	Status      TaskStatus
	Priority    int
	RetryCount  int
	MaxRetries  int
	TimeoutSecs *int // nil → no timeout
	RunAt       time.Time
	ParentID    *string // set when this task was created by a restart
	ScheduleID  *string // set when created by the scheduler
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// ── Attempt ──────────────────────────────────────────────────────────────────

// AttemptStatus is the state of a single execution attempt.
// "failed" is only valid here, never on Task.
type AttemptStatus string

const (
	AttemptStatusRunning   AttemptStatus = "running"
	AttemptStatusSucceeded AttemptStatus = "succeeded"
	AttemptStatusFailed    AttemptStatus = "failed"
	AttemptStatusCancelled AttemptStatus = "cancelled"
)

// Attempt records one execution of a Task.
// A task accumulates multiple attempts due to retries or heartbeat recovery.
type Attempt struct {
	ID          string
	TaskID      string
	AttemptNum  int // starts at 1
	WorkerID    *string
	Status      AttemptStatus
	Result      json.RawMessage // small results only; large outputs live externally
	Error       *string
	StartedAt   *time.Time
	FinishedAt  *time.Time
	HeartbeatAt *time.Time // updated directly, not via task_events
}

// ── TaskEvent ─────────────────────────────────────────────────────────────────

// TaskEventType identifies a state-transition in the task lifecycle.
type TaskEventType string

const (
	TaskEventQueued           TaskEventType = "queued"
	TaskEventStarted          TaskEventType = "started"
	TaskEventSucceeded        TaskEventType = "succeeded"
	TaskEventRetrying         TaskEventType = "retrying"
	TaskEventDead             TaskEventType = "dead"
	TaskEventHalted           TaskEventType = "halted"
	TaskEventCancelled        TaskEventType = "cancelled"
	TaskEventHeartbeatTimeout TaskEventType = "heartbeat_timeout"
	TaskEventSuperseded       TaskEventType = "superseded"
)

// TaskEvent records a state-transition point used by the dashboard timeline
// and SSE stream. Heartbeats are intentionally excluded.
type TaskEvent struct {
	ID        string
	TaskID    string
	Event     TaskEventType
	Detail    json.RawMessage // e.g. {"error": "...", "attempt_id": "...", "retry_count": 2}
	CreatedAt time.Time
}

// ── Worker ────────────────────────────────────────────────────────────────────

// WorkerType distinguishes Go goroutine workers from Python Bridge workers.
type WorkerType string

const (
	WorkerTypeGo     WorkerType = "go"
	WorkerTypePython WorkerType = "python"
)

// WorkerStatus reflects whether a worker has remaining capacity.
type WorkerStatus string

const (
	WorkerStatusIdle WorkerStatus = "idle"
	WorkerStatusBusy WorkerStatus = "busy"
)

// Worker represents a registered worker process (Go goroutine or Python Bridge).
// CurrentTaskCount is a derived field maintained exclusively by the Store layer.
type Worker struct {
	ID               string
	Type             WorkerType
	TaskTypes        []string // stored as JSON array
	Queue            string
	Concurrency      int
	CurrentTaskCount int // DO NOT write directly — use Store state transitions
	Status           WorkerStatus
	StartedAt        time.Time
	HeartbeatAt      *time.Time
	SocketPath       *string // nil for Go workers; Unix socket path for Python Bridge workers
}

// ── Schedule ──────────────────────────────────────────────────────────────────

// Schedule defines a recurring task template driven by a cron expression
// or a fixed interval. Exactly one of CronExpr or IntervalSecs must be set.
type Schedule struct {
	ID           string
	TaskType     string
	Queue        string
	Payload      json.RawMessage
	CronExpr     *string // e.g. "0 9 * * 1-5"
	IntervalSecs *int    // e.g. 3600 for every hour
	Enabled      bool
	LastRunAt    *time.Time
	NextRunAt    *time.Time
	CreatedAt    time.Time
}
