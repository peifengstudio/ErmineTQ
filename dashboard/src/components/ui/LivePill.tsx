import { useLivePill } from '../../hooks/useLivePill'

interface LivePillProps {
  /** Date.now() of the last SSE event. Pass null when not yet connected. */
  lastEventAt: number | null
}

/**
 * LivePill — "Live · updated Ns ago" indicator.
 * Green pulsing dot when connected; grey "Connecting…" when not.
 */
export function LivePill({ lastEventAt }: LivePillProps) {
  const elapsed = useLivePill(lastEventAt)

  if (elapsed === null) {
    return (
      <span className="live-pill">
        <span className="dot" style={{ background: 'var(--text-subtle)', animation: 'none' }} />
        Connecting…
      </span>
    )
  }

  const label = elapsed === 0 ? 'just now' : `${elapsed}s ago`
  return (
    <span className="live-pill">
      <span className="dot" />
      Live · updated {label}
    </span>
  )
}
