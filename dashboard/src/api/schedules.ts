import { get, patch, post } from './client'
import type { Schedule, ScheduleAction, Task } from './types'

export function listSchedules(): Promise<Schedule[]> {
  return get<Schedule[]>('/schedules')
}

export function controlSchedule(id: string, action: ScheduleAction): Promise<Schedule | Task> {
  switch (action) {
    case 'trigger':
      // POST /api/schedules/{id}/trigger → returns the new Task
      return post<Task>(`/schedules/${id}/trigger`)
    case 'enable':
      return patch<Schedule>(`/schedules/${id}`, { enabled: true })
    case 'disable':
      return patch<Schedule>(`/schedules/${id}`, { enabled: false })
  }
}
