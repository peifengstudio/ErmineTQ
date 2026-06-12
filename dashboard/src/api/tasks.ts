import { get, post } from './client'
import type { CreateTaskInput, ListTasksFilter, Task, TaskAction, TaskDetail } from './types'

export function listTasks(filter: ListTasksFilter = {}): Promise<Task[]> {
  const params: Record<string, string | number> = {}
  if (filter.status) params.status = filter.status
  if (filter.type) params.type = filter.type
  if (filter.queue) params.queue = filter.queue
  if (filter.limit) params.limit = filter.limit
  if (filter.offset) params.offset = filter.offset
  return get<Task[]>('/tasks', params)
}

export function getTask(id: string): Promise<TaskDetail> {
  return get<TaskDetail>(`/tasks/${id}`)
}

export function createTask(input: CreateTaskInput): Promise<Task> {
  return post<Task>('/tasks', input)
}

/** Returns { status: 'ok' } for most actions; returns the new Task for 'restart'. */
export function controlTask(id: string, action: TaskAction): Promise<Task | { status: string }> {
  return post(`/tasks/${id}/control`, { action })
}
