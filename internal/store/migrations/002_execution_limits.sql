-- Composite indexes to make execution-limit counting queries efficient.
--
-- When claiming a task the scheduler checks three limits:
--
--   global:     SELECT COUNT(*) FROM tasks WHERE status = 'running'
--   queue:      SELECT COUNT(*) FROM tasks WHERE status = 'running' AND queue = ?
--   task type:  SELECT COUNT(*) FROM tasks WHERE status = 'running' AND type  = ?
--
-- The global count is already served by idx_tasks_status_run_at (from 001).
-- These two new indexes allow SQLite to satisfy the queue and type counts
-- with a narrow index scan rather than a full table scan.

CREATE INDEX idx_tasks_queue_status ON tasks(queue, status);
CREATE INDEX idx_tasks_type_status  ON tasks(type,  status);
