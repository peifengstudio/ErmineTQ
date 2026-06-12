// ─── Domain types ─────────────────────────────────────────────────────────────
// Field names match Go JSON serialisation (PascalCase — no json struct tags).

export type TaskStatus =
  | 'queued'
  | 'running'
  | 'retrying'
  | 'succeeded'
  | 'dead'
  | 'halted'
  | 'cancelled'
  | 'superseded'

export type AttemptStatus = 'running' | 'succeeded' | 'failed' | 'cancelled'

export type TaskEventType =
  | 'queued'
  | 'started'
  | 'succeeded'
  | 'retrying'
  | 'dead'
  | 'halted'
  | 'cancelled'
  | 'heartbeat_timeout'
  | 'superseded'

export type WorkerType = 'go' | 'python'
export type WorkerStatus = 'idle' | 'busy'

export interface Task {
  ID: string
  Type: string
  Queue: string
  Payload: unknown
  Status: TaskStatus
  Priority: number
  RetryCount: number
  MaxRetries: number
  TimeoutSecs: number | null
  RunAt: string // ISO 8601
  ParentID: string | null
  ScheduleID: string | null
  CreatedAt: string
  UpdatedAt: string
}

export interface Attempt {
  ID: string
  TaskID: string
  AttemptNum: number
  WorkerID: string | null
  Status: AttemptStatus
  Result: unknown | null
  Error: string | null
  StartedAt: string | null
  FinishedAt: string | null
  HeartbeatAt: string | null
}

export interface TaskEvent {
  ID: string
  TaskID: string
  Event: TaskEventType
  Detail: unknown | null
  CreatedAt: string
}

export interface TaskDetail {
  task: Task
  attempts: Attempt[]
  events: TaskEvent[]
}

export interface Worker {
  ID: string
  Type: WorkerType
  TaskTypes: string[]
  Queue: string
  Concurrency: number
  CurrentTaskCount: number
  Status: WorkerStatus
  StartedAt: string
  HeartbeatAt: string | null
  SocketPath: string | null
}

export interface Schedule {
  ID: string
  TaskType: string
  Queue: string
  Payload: unknown
  CronExpr: string | null
  IntervalSecs: number | null
  Enabled: boolean
  LastRunAt: string | null
  NextRunAt: string | null
  CreatedAt: string
}

// SSE event payload from GET /api/events
export interface SSEEvent {
  task_id: string
  event: TaskEventType
  detail: unknown | null
  timestamp: string
}

// ─── Request shapes ────────────────────────────────────────────────────────────

export interface ListTasksFilter {
  status?: TaskStatus
  type?: string
  queue?: string
  limit?: number
  offset?: number
}

export interface CreateTaskInput {
  type: string
  queue?: string
  payload?: unknown
  priority?: number
  max_retries?: number
  timeout_secs?: number
  run_at?: string
}

export type TaskAction = 'halt' | 'resume' | 'cancel' | 'retry' | 'restart'
export type ScheduleAction = 'trigger' | 'enable' | 'disable'
