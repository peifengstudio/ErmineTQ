package bridge

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

// ── wire protocol types ───────────────────────────────────────────────────────

// callRequest is the JSON object sent to the Python Bridge for a task call.
type callRequest struct {
	TaskID  string          `json:"task_id"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// cancelRequest is the JSON object sent to cancel a running task.
type cancelRequest struct {
	Type   string `json:"type"` // always "cancel"
	TaskID string `json:"task_id"`
}

// callResponse is the JSON object received from the Python Bridge.
// Exactly one of Result or Error will be set.
type callResponse struct {
	Result json.RawMessage `json:"result,omitempty"`
	Error  string          `json:"error,omitempty"`
}

// ── backoff parameters ────────────────────────────────────────────────────────

const (
	backoffBase = 500 * time.Millisecond
	backoffMax  = 30 * time.Second
)

func nextBackoff(current time.Duration) time.Duration {
	next := current * 2
	if next > backoffMax {
		next = backoffMax
	}
	return next
}

// ── Client ────────────────────────────────────────────────────────────────────

// Client manages a single persistent Unix socket connection to a Python Bridge
// process. Calls are serialised: only one RPC is in flight at a time.
//
// The connection is established lazily on the first Call and re-established
// automatically after any failure using exponential back-off.
type Client struct {
	socketPath string

	mu     sync.Mutex // serialises all conn access and Call operations
	conn   net.Conn
	reader *bufio.Reader
	closed bool
}

// NewClient creates a Client for the Unix socket at socketPath.
// The connection is not established until the first Call.
func NewClient(socketPath string) *Client {
	return &Client{socketPath: socketPath}
}

// Call sends a task to the Python Bridge and waits for the result.
// It respects ctx cancellation and deadline. On connection errors it retries
// with exponential back-off; ctx cancellation aborts the retry loop.
//
// Returns (resultJSON, nil) on success, or (nil, error) on failure.
func (c *Client) Call(ctx context.Context, taskID, taskType string, payload []byte) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, errors.New("bridge client is closed")
	}

	// Ensure we have a working connection, retrying with backoff if needed.
	if err := c.ensureConn(ctx); err != nil {
		return nil, fmt.Errorf("bridge connect: %w", err)
	}

	req := callRequest{
		TaskID:  taskID,
		Type:    taskType,
		Payload: json.RawMessage(payload),
	}
	line, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}
	line = append(line, '\n')

	// Clear any stale deadline before writing (no per-call deadline on the
	// connection; ctx cancellation drives timeout instead — see below).
	_ = c.conn.SetDeadline(time.Time{})

	if _, err := c.conn.Write(line); err != nil {
		c.markBroken()
		return nil, fmt.Errorf("write request: %w", err)
	}

	// Capture reader before spawning the goroutine so the goroutine holds a
	// stable pointer even if markBroken() sets c.reader = nil concurrently.
	type readResult struct {
		data string
		err  error
	}
	ch := make(chan readResult, 1)
	reader := c.reader
	go func() {
		data, err := reader.ReadString('\n')
		ch <- readResult{data, err}
	}()

	// ctx cancellation is the sole timeout mechanism.  When ctx.Done() fires
	// we close the connection (markBroken), which unblocks the goroutine's
	// ReadString with an error.  We return ctx.Err() directly, so callers
	// always see context.DeadlineExceeded or context.Canceled — not a raw
	// network timeout — when the ctx expires.
	select {
	case <-ctx.Done():
		c.markBroken()
		return nil, ctx.Err()
	case rr := <-ch:
		if rr.err != nil {
			c.markBroken()
			return nil, fmt.Errorf("read response: %w", rr.err)
		}
		var resp callResponse
		if err := json.Unmarshal([]byte(rr.data), &resp); err != nil {
			return nil, fmt.Errorf("unmarshal response: %w", err)
		}
		if resp.Error != "" {
			return nil, errors.New(resp.Error)
		}
		return []byte(resp.Result), nil
	}
}

// Cancel sends a fire-and-forget cancel signal to the Python Bridge.
// If the connection is unavailable or the write fails the error is silently
// discarded — the HeartbeatScanner will recover stale attempts regardless.
func (c *Client) Cancel(taskID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed || c.conn == nil {
		return
	}

	req := cancelRequest{Type: "cancel", TaskID: taskID}
	line, err := json.Marshal(req)
	if err != nil {
		return
	}
	line = append(line, '\n')

	_ = c.conn.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := c.conn.Write(line); err != nil {
		c.markBroken()
	}
}

// Close permanently closes the client and its underlying connection.
// Any in-flight Call will return an error. Subsequent calls to Call return
// an error immediately.
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.closed = true
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
		c.reader = nil
	}
}

// ── private helpers ───────────────────────────────────────────────────────────

// ensureConn dials (or re-dials after failure) the Unix socket.
// It retries with exponential back-off until the connection succeeds or ctx
// is cancelled. Must be called with c.mu held.
func (c *Client) ensureConn(ctx context.Context) error {
	if c.conn != nil {
		return nil // already connected
	}

	backoff := backoffBase
	for {
		conn, err := (&net.Dialer{}).DialContext(ctx, "unix", c.socketPath)
		if err == nil {
			c.conn = conn
			c.reader = bufio.NewReader(conn)
			return nil
		}

		// If the context is already done, return immediately.
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Wait for backoff or context cancellation.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = nextBackoff(backoff)
	}
}

// markBroken closes the connection so the next Call triggers a re-dial.
// Must be called with c.mu held.
func (c *Client) markBroken() {
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
		c.reader = nil
	}
}
