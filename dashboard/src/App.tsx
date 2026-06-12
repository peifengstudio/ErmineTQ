import { useEffect } from 'react'
import {
  faCalendar,
  faListCheck,
  faMicrochip,
  faPause,
  faPlay,
  faRotateRight,
  faTableColumns,
  faX,
} from '@fortawesome/free-solid-svg-icons'
import { Button, Icon, IdChip, LivePill, StatusBadge } from './components/ui'
import { useSSE } from './hooks/useSSE'
import { useAppStore } from './store/useAppStore'

const NAV = [
  { label: 'Overview', icon: faTableColumns },
  { label: 'Tasks', icon: faListCheck },
  { label: 'Workers', icon: faMicrochip },
  { label: 'Schedules', icon: faCalendar },
]

export default function App() {
  const { tasks, workers, schedules, counts, lastSSEAt, fetchAll, handleSSEEvent } = useAppStore()

  // Initial load
  useEffect(() => {
    fetchAll()
  }, [fetchAll])

  // SSE — on reconnect, re-fetch full state
  const { connectedAt } = useSSE(handleSSEEvent)
  useEffect(() => {
    if (connectedAt !== null) fetchAll()
  }, [connectedAt, fetchAll])

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

        {NAV.map(({ label, icon }) => (
          <div key={label} className="sb-item">
            <Icon icon={icon} size="md" />
            <span>{label}</span>
          </div>
        ))}

        <div className="sb-section">Quick filters</div>
        <div className="sb-item">
          <span
            style={{
              width: 6,
              height: 6,
              borderRadius: '50%',
              background: 'var(--st-running)',
              display: 'inline-block',
            }}
          />
          <span>Running</span>
          <span className="sb-item-count">{counts.running}</span>
        </div>
        <div className="sb-item">
          <span
            style={{
              width: 6,
              height: 6,
              borderRadius: '50%',
              background: 'var(--st-retrying)',
              display: 'inline-block',
            }}
          />
          <span>Retrying</span>
          <span className="sb-item-count">{counts.retrying}</span>
        </div>
        <div className="sb-item">
          <span
            style={{
              width: 6,
              height: 6,
              borderRadius: '50%',
              background: 'var(--st-dead)',
              display: 'inline-block',
            }}
          />
          <span>Dead-letter</span>
          <span className="sb-item-count">{counts.dead}</span>
        </div>

        <div className="sb-footer">
          <div className="sb-server">
            <span className="sb-server-dot" />
            <span>Engine</span>
          </div>
        </div>
      </aside>

      {/* Main */}
      <main className="main">
        <div className="topbar">
          <div className="topbar-crumb">
            <span className="topbar-crumb-muted">ErmineTQ</span>
            <span className="topbar-crumb-sep">/</span>
            <span className="topbar-crumb-active">Overview</span>
          </div>
          <div className="topbar-right">
            <LivePill lastEventAt={lastSSEAt} />
            <span className="topbar-mono">localhost:8080</span>
          </div>
        </div>

        <div className="content">
          <div className="content-narrow flex flex-col gap-6">
            <div className="page-head">
              <div>
                <h1 className="page-title">Overview</h1>
                <p className="page-sub">
                  {counts.total} tasks · {workers.length} workers · {schedules.length} schedules
                </p>
              </div>
            </div>

            {/* Stat tiles */}
            <div className="grid-4">
              <div className="stat">
                <div className="stat-label">Running</div>
                <div className="stat-value" style={{ color: 'var(--st-running)' }}>
                  {counts.running}
                </div>
                <div className="stat-delta">{counts.queued} queued</div>
              </div>
              <div className="stat">
                <div className="stat-label">Succeeded</div>
                <div className="stat-value" style={{ color: 'var(--st-succeeded)' }}>
                  {counts.succeeded}
                </div>
                <div className="stat-delta">{counts.retrying} retrying</div>
              </div>
              <div className="stat">
                <div className="stat-label">Dead-letter</div>
                <div className="stat-value" style={{ color: 'var(--st-dead)' }}>
                  {counts.dead}
                </div>
                <div className="stat-delta">
                  {counts.dead > 0 ? 'requires attention' : 'all clear'}
                </div>
              </div>
              <div className="stat">
                <div className="stat-label">Halted</div>
                <div className="stat-value" style={{ color: 'var(--st-halted)' }}>
                  {counts.halted}
                </div>
                <div className="stat-delta">{counts.cancelled} cancelled</div>
              </div>
            </div>

            {/* Tasks table */}
            <div className="card">
              <div className="card-head">
                <span className="card-title">Tasks</span>
                <span className="card-sub ml-auto">{counts.total} total</span>
              </div>
              <div className="table-wrap">
                <table className="table">
                  <thead>
                    <tr>
                      <th>Task</th>
                      <th>Queue</th>
                      <th>Status</th>
                      <th>Retries</th>
                      <th>Updated</th>
                      <th></th>
                    </tr>
                  </thead>
                  <tbody>
                    {tasks.length === 0 && (
                      <tr>
                        <td colSpan={6} className="py-10 text-center text-text-muted">
                          No tasks yet
                        </td>
                      </tr>
                    )}
                    {tasks.slice(0, 20).map((task) => (
                      <tr key={task.ID}>
                        <td>
                          <div className="flex flex-col gap-1">
                            <span className="font-medium">{task.Type}</span>
                            <IdChip id={task.ID} truncate={16} />
                          </div>
                        </td>
                        <td className="mono">{task.Queue || 'default'}</td>
                        <td>
                          <StatusBadge status={task.Status} />
                        </td>
                        <td className="mono">
                          {task.RetryCount}/{task.MaxRetries}
                        </td>
                        <td className="mono muted">
                          {new Date(task.UpdatedAt).toLocaleTimeString()}
                        </td>
                        <td>
                          <div className="row-actions">
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
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>

            {/* Workers */}
            <div className="card">
              <div className="card-head">
                <span className="card-title">Workers</span>
                <span className="card-sub ml-auto">{workers.length} registered</span>
              </div>
              <div className="table-wrap">
                <table className="table">
                  <thead>
                    <tr>
                      <th>ID</th>
                      <th>Type</th>
                      <th>Queue</th>
                      <th>Concurrency</th>
                      <th>Status</th>
                    </tr>
                  </thead>
                  <tbody>
                    {workers.length === 0 && (
                      <tr>
                        <td colSpan={5} className="py-10 text-center text-text-muted">
                          No workers registered
                        </td>
                      </tr>
                    )}
                    {workers.map((w) => (
                      <tr key={w.ID}>
                        <td>
                          <IdChip id={w.ID} truncate={16} />
                        </td>
                        <td className="mono">{w.Type}</td>
                        <td className="mono">{w.Queue || 'default'}</td>
                        <td className="mono">
                          {w.CurrentTaskCount}/{w.Concurrency}
                        </td>
                        <td>
                          <StatusBadge status={w.Status} />
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>

            {/* Schedules */}
            <div className="card">
              <div className="card-head">
                <span className="card-title">Schedules</span>
                <span className="card-sub ml-auto">{schedules.length} total</span>
              </div>
              <div className="table-wrap">
                <table className="table">
                  <thead>
                    <tr>
                      <th>ID</th>
                      <th>Type</th>
                      <th>Cadence</th>
                      <th>Next run</th>
                      <th>Enabled</th>
                    </tr>
                  </thead>
                  <tbody>
                    {schedules.length === 0 && (
                      <tr>
                        <td colSpan={5} className="py-10 text-center text-text-muted">
                          No schedules
                        </td>
                      </tr>
                    )}
                    {schedules.map((sc) => (
                      <tr key={sc.ID}>
                        <td>
                          <IdChip id={sc.ID} truncate={16} />
                        </td>
                        <td>{sc.TaskType}</td>
                        <td className="mono">
                          {sc.CronExpr ?? (sc.IntervalSecs ? `every ${sc.IntervalSecs}s` : '—')}
                        </td>
                        <td className="mono muted">
                          {sc.NextRunAt ? new Date(sc.NextRunAt).toLocaleString() : '—'}
                        </td>
                        <td>
                          <StatusBadge status={sc.Enabled ? 'running' : 'halted'} />
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        </div>
      </main>
    </div>
  )
}
