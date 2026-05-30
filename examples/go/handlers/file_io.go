package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// ── go_file_write ─────────────────────────────────────────────────────────────

type fileWritePayload struct {
	Filename string `json:"filename"` // written under examples/output/
	Content  string `json:"content"`
	Append   bool   `json:"append"` // default false (overwrite)
}

type fileWriteResult struct {
	Path         string `json:"path"`
	BytesWritten int    `json:"bytes_written"`
	Mode         string `json:"mode"` // "write" or "append"
}

// FileWrite writes (or appends) content to a file under examples/output/.
//
// Payload: {"filename": "hello.txt", "content": "Hello!\n", "append": false}
// Output:  examples/output/<filename>
func FileWrite(ctx context.Context, payload []byte) ([]byte, error) {
	var p fileWritePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("invalid payload: %w", err)
	}
	if p.Filename == "" {
		return nil, fmt.Errorf("filename is required")
	}
	// Sanitise: no path traversal
	if strings.Contains(p.Filename, "..") || filepath.IsAbs(p.Filename) {
		return nil, fmt.Errorf("filename must be a relative name without ..")
	}

	outPath := filepath.Join(outputDir(), p.Filename)
	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		return nil, fmt.Errorf("mkdir: %w", err)
	}

	flag := os.O_CREATE | os.O_WRONLY
	if p.Append {
		flag |= os.O_APPEND
	} else {
		flag |= os.O_TRUNC
	}

	f, err := os.OpenFile(outPath, flag, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", outPath, err)
	}
	defer f.Close()

	n, err := f.WriteString(p.Content)
	if err != nil {
		return nil, fmt.Errorf("write: %w", err)
	}

	mode := "write"
	if p.Append {
		mode = "append"
	}

	return json.Marshal(fileWriteResult{
		Path:         outPath,
		BytesWritten: n,
		Mode:         mode,
	})
}

// ── go_file_read ──────────────────────────────────────────────────────────────

type fileReadPayload struct {
	Path     string `json:"path"`      // path to read (relative to cwd)
	MaxBytes int    `json:"max_bytes"` // default 65536
}

type fileReadResult struct {
	Path       string `json:"path"`
	SizeBytes  int64  `json:"size_bytes"`
	Content    string `json:"content"`
	Truncated  bool   `json:"truncated"`
	OutputFile string `json:"output_file"` // log copy written here
}

// FileRead reads a file and writes a log copy to examples/output/.
//
// Payload: {"path": "examples/output/hello.txt"}
// Output:  examples/output/go_file_read_<timestamp>.txt
func FileRead(_ context.Context, payload []byte) ([]byte, error) {
	var p fileReadPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("invalid payload: %w", err)
	}
	if p.Path == "" {
		return nil, fmt.Errorf("path is required")
	}
	if p.MaxBytes <= 0 {
		p.MaxBytes = 64 * 1024
	}

	info, err := os.Stat(p.Path)
	if err != nil {
		return nil, fmt.Errorf("stat %s: %w", p.Path, err)
	}

	data, err := os.ReadFile(p.Path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", p.Path, err)
	}

	truncated := false
	content := data
	if len(content) > p.MaxBytes {
		content = content[:p.MaxBytes]
		truncated = true
	}

	logContent := fmt.Sprintf(
		"=== go_file_read log ===\npath:      %s\nsize:      %d bytes\nread_at:   %s\ntruncated: %v\n\n--- content ---\n%s",
		p.Path, info.Size(), time.Now().Format(time.RFC3339), truncated, content,
	)
	outPath, err := writeOutputText(
		fmt.Sprintf("go_file_read_%d.txt", time.Now().UnixNano()),
		logContent,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(fileReadResult{
		Path:       p.Path,
		SizeBytes:  info.Size(),
		Content:    string(content),
		Truncated:  truncated,
		OutputFile: outPath,
	})
}
