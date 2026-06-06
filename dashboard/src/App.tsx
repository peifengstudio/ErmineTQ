import {
  faBolt,
  faCalendar,
  faChartBar,
  faListCheck,
  faMicrochip,
  faPause,
  faPlay,
  faRotateRight,
  faTableColumns,
  faX,
} from '@fortawesome/free-solid-svg-icons'
import { Button, Icon, IdChip, LivePill, StatusBadge } from './components/ui'
import type { Status } from './components/ui'

const STATUSES: Status[] = [
  'queued',
  'running',
  'retrying',
  'succeeded',
  'dead',
  'halted',
  'cancelled',
  'superseded',
]

const NAV: Array<{ label: string; icon: typeof faBolt; count?: number }> = [
  { label: 'Overview', icon: faTableColumns },
  { label: 'Tasks', icon: faListCheck, count: 42 },
  { label: 'Workers', icon: faMicrochip, count: 3 },
  { label: 'Schedules', icon: faCalendar, count: 5 },
]

const STATS = [
  { label: 'Running', value: '3', sub: '12 queued', status: 'running' as Status },
  { label: 'Throughput', value: '4.2', sub: '/min', status: undefined },
  { label: 'Success rate', value: '98%', sub: '1 failed', status: undefined },
  { label: 'Dead-letter', value: '2', sub: 'requires attention', status: 'dead' as Status },
]

// Placeholder shell — will be replaced in M3
export default function App() {
  return (
    <div className="app">
      {/* Sidebar */}
      <aside className="sidebar">
        <div className="sb-brand">
          <div className="sb-logo">E</div>
          <div className="sb-brand-text">
            <b>ErmineTQ</b>
            <small>v0.1 · :8080</small>
          </div>
        </div>

        {NAV.map(({ label, icon, count }) => (
          <div key={label} className="sb-item">
            <Icon icon={icon} size="md" />
            <span>{label}</span>
            {count != null && <span className="sb-item-count">{count}</span>}
          </div>
        ))}

        <div className="sb-footer">
          <div className="sb-server">
            <span className="sb-server-dot" />
            <span>Engine</span>
            <span className="sb-server-meta">62 MB</span>
          </div>
        </div>
      </aside>

      {/* Main */}
      <main className="main">
        <div className="topbar">
          <div className="topbar-crumb">
            <span className="topbar-crumb-muted">ErmineTQ</span>
            <span className="topbar-crumb-sep">/</span>
            <span className="topbar-crumb-active">Component Preview</span>
          </div>
          <div className="topbar-right">
            <LivePill lastEventAt={Date.now()} />
            <span className="topbar-mono">localhost:8080</span>
          </div>
        </div>

        <div className="content">
          <div className="content-narrow flex flex-col gap-8">
            <div className="page-head">
              <div>
                <h1 className="page-title">M0 Component Preview</h1>
                <p className="page-sub">Tailwind CSS v4 · Font Awesome · Prettier — all wired up</p>
              </div>
              <Button variant="primary">
                <Icon icon={faBolt} size="sm" />
                New task
              </Button>
            </div>

            {/* Status badges */}
            <div className="card">
              <div className="card-head">
                <Icon icon={faChartBar} size="md" className="text-text-muted" />
                <span className="card-title">StatusBadge</span>
              </div>
              <div className="flex flex-wrap gap-2 p-4">
                {STATUSES.map((s) => (
                  <StatusBadge key={s} status={s} />
                ))}
              </div>
            </div>

            {/* IdChip */}
            <div className="card">
              <div className="card-head">
                <span className="card-title">IdChip — click to copy</span>
              </div>
              <div className="flex flex-wrap items-center gap-3 p-4">
                <IdChip id="t_a1b2c3d4" />
                <IdChip id="t_e5f6g7h8i9j0" truncate={10} />
                <IdChip id="w_python_001" />
              </div>
            </div>

            {/* Buttons */}
            <div className="card">
              <div className="card-head">
                <span className="card-title">Button variants</span>
              </div>
              <div className="flex flex-wrap items-center gap-2 p-4">
                <Button variant="default">Default</Button>
                <Button variant="primary">
                  <Icon icon={faBolt} size="sm" />
                  Primary
                </Button>
                <Button variant="ghost">Ghost</Button>
                <Button variant="icon" tip="Halt">
                  <Icon icon={faPause} size="sm" />
                </Button>
                <Button variant="icon" tip="Resume">
                  <Icon icon={faPlay} size="sm" />
                </Button>
                <Button variant="icon" tip="Retry">
                  <Icon icon={faRotateRight} size="sm" />
                </Button>
                <Button variant="icon-danger" tip="Cancel">
                  <Icon icon={faX} size="sm" />
                </Button>
              </div>
            </div>

            {/* Stat tiles */}
            <div className="grid-4">
              {STATS.map(({ label, value, sub, status }) => (
                <div key={label} className="stat">
                  <div className="stat-label">{label}</div>
                  <div
                    className="stat-value"
                    style={status ? { color: `var(--st-${status})` } : undefined}
                  >
                    {value}
                  </div>
                  <div className="stat-delta">{sub}</div>
                </div>
              ))}
            </div>
          </div>
        </div>
      </main>
    </div>
  )
}
