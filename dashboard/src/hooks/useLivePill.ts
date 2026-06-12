import { useEffect, useState } from 'react'

/**
 * Returns the number of whole seconds elapsed since `lastEventAt`.
 * Re-renders every second. Pass `null` when not yet connected.
 */
export function useLivePill(lastEventAt: number | null): number | null {
  const [elapsed, setElapsed] = useState<number | null>(
    lastEventAt !== null ? Math.floor((Date.now() - lastEventAt) / 1_000) : null,
  )

  useEffect(() => {
    if (lastEventAt === null) {
      setElapsed(null)
      return
    }
    setElapsed(Math.floor((Date.now() - lastEventAt) / 1_000))
    const id = setInterval(() => {
      setElapsed(Math.floor((Date.now() - lastEventAt) / 1_000))
    }, 1_000)
    return () => clearInterval(id)
  }, [lastEventAt])

  return elapsed
}
