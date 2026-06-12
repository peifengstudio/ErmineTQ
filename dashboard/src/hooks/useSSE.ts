import { useEffect, useRef, useState } from 'react'
import type { SSEEvent } from '../api/types'

const SSE_URL = '/api/events'
// Backoff: 1s → 2s → 4s → … capped at 30s
const MAX_BACKOFF_MS = 30_000

interface UseSSEResult {
  lastEvent: SSEEvent | null
  connectedAt: number | null // Date.now() when the connection was established
}

/**
 * useSSE — connects to GET /api/events and returns the most recent SSEEvent.
 * Reconnects automatically with exponential backoff on any disconnect.
 *
 * The caller is responsible for re-fetching full state via REST after reconnect
 * (SSE is not a reliable stream — missed events are not replayed).
 */
export function useSSE(onEvent: (ev: SSEEvent) => void): UseSSEResult {
  const [lastEvent, setLastEvent] = useState<SSEEvent | null>(null)
  const [connectedAt, setConnectedAt] = useState<number | null>(null)

  // Keep a stable ref to the callback so the effect never needs to re-run
  // when the caller's inline function identity changes.
  const onEventRef = useRef(onEvent)
  onEventRef.current = onEvent

  useEffect(() => {
    let es: EventSource | null = null
    let backoffMs = 1_000
    let retryTimer: ReturnType<typeof setTimeout> | null = null
    let cancelled = false

    function connect() {
      es = new EventSource(SSE_URL)

      es.onopen = () => {
        backoffMs = 1_000 // reset on successful connect
        setConnectedAt(Date.now())
      }

      es.onmessage = (e: MessageEvent<string>) => {
        try {
          const ev = JSON.parse(e.data) as SSEEvent
          setLastEvent(ev)
          onEventRef.current(ev)
        } catch {
          // Malformed event — ignore, stay connected
        }
      }

      es.onerror = () => {
        es?.close()
        es = null
        setConnectedAt(null)
        if (cancelled) return
        retryTimer = setTimeout(() => {
          backoffMs = Math.min(backoffMs * 2, MAX_BACKOFF_MS)
          if (!cancelled) connect()
        }, backoffMs)
      }
    }

    connect()

    return () => {
      cancelled = true
      es?.close()
      if (retryTimer !== null) clearTimeout(retryTimer)
    }
  }, []) // intentionally empty — connection is managed for the app lifetime

  return { lastEvent, connectedAt }
}
