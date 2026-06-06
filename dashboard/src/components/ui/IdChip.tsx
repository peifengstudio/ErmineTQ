import { useState, useCallback } from 'react'

interface IdChipProps {
  id: string
  /** How many characters to show. Defaults to full id. */
  truncate?: number
  className?: string
}

/**
 * IdChip — monospace ID pill with click-to-copy.
 * Tooltip flips from the raw id to "Copied ✓" for 1.5 s.
 */
export function IdChip({ id, truncate, className = '' }: IdChipProps) {
  const [copied, setCopied] = useState(false)

  const label = truncate ? id.slice(0, truncate) : id

  const handleClick = useCallback(
    (e: React.MouseEvent) => {
      e.stopPropagation()
      navigator.clipboard.writeText(id).then(() => {
        setCopied(true)
        setTimeout(() => setCopied(false), 1500)
      })
    },
    [id],
  )

  return (
    <span
      className={`id-chip ${className}`}
      data-tip={copied ? 'Copied ✓' : id}
      onClick={handleClick}
      role="button"
      tabIndex={0}
      onKeyDown={(e) => e.key === 'Enter' && handleClick(e as never)}
    >
      {label}
    </span>
  )
}
