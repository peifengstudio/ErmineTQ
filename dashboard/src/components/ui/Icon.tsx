import { FontAwesomeIcon } from '@fortawesome/react-fontawesome'
import type { FontAwesomeIconProps } from '@fortawesome/react-fontawesome'
import type { IconDefinition } from '@fortawesome/fontawesome-svg-core'

/** Design-system size aliases (avoids collision with FA's own SizeProp) */
type IconSize = 'xs' | 'sm' | 'md' | 'lg'

interface IconProps extends Omit<FontAwesomeIconProps, 'icon' | 'size'> {
  icon: IconDefinition
  /** Convenience size variants matching the design-system spacing rhythm */
  size?: IconSize
}

const sizeClass: Record<IconSize, string> = {
  xs: 'size-2.5', // 10 px
  sm: 'size-3', // 12 px — table / row-action icons
  md: 'size-3.5', // 14 px — sidebar / buttons  (default)
  lg: 'size-4', // 16 px
}

/**
 * Icon — thin wrapper around FontAwesomeIcon.
 * Applies design-system sizing via Tailwind and inherits `currentColor`
 * so icons follow their parent's text colour automatically.
 */
export function Icon({ icon, size = 'md', className = '', ...rest }: IconProps) {
  return (
    <FontAwesomeIcon
      icon={icon}
      className={`${sizeClass[size]} shrink-0 ${className}`.trim()}
      {...rest}
    />
  )
}
