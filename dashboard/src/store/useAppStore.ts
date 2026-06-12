import { create } from 'zustand'
import { listTasks } from '../api/tasks'
import { listWorkers } from '../api/workers'
import { listSchedules } from '../api/schedules'
import { getTask } from '../api/tasks'
import type { Schedule, SSEEvent, Task, TaskStatus, Worker } from '../api/types'

// ─── Throughput bucket ────────────────────────────────────────────────────────

/** One-minute bucket of succeeded / failed counts for the throughput chart. */
export interface ThroughputBucket {
  minuteKey: number // Math.floor(Date.now() / 60_000)
  succeeded: number
  failed: number
}

function currentMinuteKey() {
  return Math.floor(Date.now() / 60_000)
}

function emptyBuckets(count = 60): ThroughputBucket[] {
  const now = currentMinuteKey()
  return Array.from({ length: count }, (_, i) => ({
    minuteKey: now - (count - 1 - i),
    succeeded: 0,
    failed: 0,
  }))
}

// ─── Derived counts ───────────────────────────────────────────────────────────

export interface StatusCounts {
  total: number
  queued: number
  running: number
  retrying: number
  succeeded: number
  dead: number
  halted: number
  cancelled: number
  superseded: number
}

function deriveCounts(tasks: Task[]): StatusCounts {
  const counts: StatusCounts = {
    total: tasks.length,
    queued: 0,
    running: 0,
    retrying: 0,
    succeeded: 0,
    dead: 0,
    halted: 0,
    cancelled: 0,
    superseded: 0,
  }
  for (const t of tasks) {
    const s = t.Status as TaskStatus
    if (s in counts) (counts as unknown as Record<string, number>)[s]++
  }
  return counts
}

// ─── Store definition ─────────────────────────────────────────────────────────

interface AppState {
  // Server data
  tasks: Task[]
  workers: Worker[]
  schedules: Schedule[]
  throughputBuckets: ThroughputBucket[]

  // Derived (recomputed on every task mutation)
  counts: StatusCounts

  // Meta
  lastSSEAt: number | null // Date.now() of the last received SSE event

  // Actions
  fetchAll: () => Promise<void>
  handleSSEEvent: (ev: SSEEvent) => Promise<void>
  tickBuckets: () => void
  setWorkers: (workers: Worker[]) => void
  setSchedules: (schedules: Schedule[]) => void
  patchSchedule: (updated: Schedule) => void
}

export const useAppStore = create<AppState>()((set, get) => ({
  tasks: [],
  workers: [],
  schedules: [],
  throughputBuckets: emptyBuckets(),
  counts: deriveCounts([]),
  lastSSEAt: null,

  // ── fetchAll — called on mount and after SSE reconnect ──────────────────
  fetchAll: async () => {
    const [tasks, workers, schedules] = await Promise.all([
      listTasks({ limit: 200 }),
      listWorkers(),
      listSchedules(),
    ])
    set({
      tasks,
      workers,
      schedules,
      counts: deriveCounts(tasks),
    })
  },

  // ── handleSSEEvent — called for every SSE message ───────────────────────
  handleSSEEvent: async (ev: SSEEvent) => {
    set({ lastSSEAt: Date.now() })

    // Re-fetch the specific task so the store always has the latest state.
    try {
      const { task } = await getTask(ev.task_id)
      set((s) => {
        const existing = s.tasks.findIndex((t) => t.ID === task.ID)
        const tasks =
          existing >= 0 ? s.tasks.map((t) => (t.ID === task.ID ? task : t)) : [task, ...s.tasks]
        return { tasks, counts: deriveCounts(tasks) }
      })
    } catch {
      // Task may have been deleted or not yet visible — ignore
    }

    // Roll throughput bucket for succeeded / failed events
    if (ev.event === 'succeeded' || ev.event === 'dead') {
      get().tickBuckets()
      const field = ev.event === 'succeeded' ? 'succeeded' : 'failed'
      const key = currentMinuteKey()
      set((s) => ({
        throughputBuckets: s.throughputBuckets.map((b) =>
          b.minuteKey === key ? { ...b, [field]: b[field] + 1 } : b,
        ),
      }))
    }
  },

  // ── tickBuckets — advance window if the minute has rolled over ──────────
  tickBuckets: () => {
    set((s) => {
      const key = currentMinuteKey()
      const last = s.throughputBuckets[s.throughputBuckets.length - 1]
      if (!last || last.minuteKey >= key) return {}

      // Advance: drop old buckets, fill gaps with zeros, append current minute
      const gap = key - last.minuteKey
      const newBuckets = [...s.throughputBuckets]
      for (let i = 0; i < gap; i++) {
        newBuckets.push({ minuteKey: last.minuteKey + i + 1, succeeded: 0, failed: 0 })
      }
      // Keep only the last 60 buckets
      return { throughputBuckets: newBuckets.slice(-60) }
    })
  },

  setWorkers: (workers) => set({ workers }),
  setSchedules: (schedules) => set({ schedules }),
  patchSchedule: (updated) =>
    set((s) => ({
      schedules: s.schedules.map((sc) => (sc.ID === updated.ID ? updated : sc)),
    })),
}))
