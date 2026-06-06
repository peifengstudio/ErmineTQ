import { useState, useEffect } from 'react'

interface LivePillProps {
  /** Timestamp of the last received SSE event (ms). Pass Date.now() on each event. */
  lastEventAt: number
}

/**
 * LivePill — "Live · updated Ns ago" indicator.
 * Green pulsing dot; counter ticks every second.
 */
export function LivePill({ lastEventAt }: LivePillProps) {
  const [elapsed, setElapsed] = useState(0)

  useEffect(() => {
    setElapsed(0)
    const id = setInterval(() => {
      setElapsed(Math.floor((Date.now() - lastEventAt) / 1000))
    }, 1000)
    return () => clearInterval(id)
  }, [lastEventAt])

  const label = elapsed === 0 ? 'just now' : `${elapsed}s ago`

  return (
    <span className="live-pill">
      <span className="dot" />
      Live · updated {label}
    </span>
  )
}
