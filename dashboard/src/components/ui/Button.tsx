import type { ButtonHTMLAttributes, ReactNode } from 'react'

type Variant = 'default' | 'primary' | 'ghost' | 'icon' | 'icon-danger'

interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: Variant
  /** Tooltip shown via CSS [data-tip] */
  tip?: string
  children: ReactNode
}

const variantClass: Record<Variant, string> = {
  default: 'btn',
  primary: 'btn primary',
  ghost: 'btn ghost',
  icon: 'btn-icon',
  'icon-danger': 'btn-icon danger',
}

/**
 * Button — wraps the design-system `.btn` / `.btn-icon` classes.
 * Use `variant="icon"` for 26×26 icon-only buttons (row actions, close, etc.).
 */
export function Button({
  variant = 'default',
  tip,
  className = '',
  children,
  ...rest
}: ButtonProps) {
  return (
    <button className={`${variantClass[variant]} ${className}`} data-tip={tip} {...rest}>
      {children}
    </button>
  )
}
