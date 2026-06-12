import { get } from './client'
import type { Worker } from './types'

export function listWorkers(): Promise<Worker[]> {
  return get<Worker[]>('/workers')
}
