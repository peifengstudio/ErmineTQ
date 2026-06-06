/** All valid task / attempt / worker statuses in the domain model */
export type Status =
  | 'queued'
  | 'running'
  | 'retrying'
  | 'succeeded'
  | 'dead'
  | 'halted'
  | 'cancelled'
  | 'superseded'
  // attempt-only
  | 'failed'
  // worker-only
  | 'idle'
  | 'busy'

interface StatusBadgeProps {
  status: Status
  className?: string
}

/**
 * StatusBadge — renders a coloured dot + label pill.
 * Styling is entirely via the `.badge[data-status]` CSS rules in app.css.
 * The running dot blinks and the live/server dot pulses (defined in tokens).
 */
export function StatusBadge({ status, className = '' }: StatusBadgeProps) {
  return (
    <span className={`badge ${className}`} data-status={status}>
      <span className="dot" />
      {status}
    </span>
  )
}
