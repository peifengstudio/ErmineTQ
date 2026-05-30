-- Add optional Unix socket path for Python Bridge workers.
-- NULL for Go goroutine workers; set for python-type workers registered
-- via the bridge. Used by the Bridge client to route Call() requests.
ALTER TABLE workers ADD COLUMN socket_path TEXT;
