package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

// httpFetchPayload is the expected task payload for go_http_fetch.
type httpFetchPayload struct {
	URL         string `json:"url"`
	TimeoutSecs int    `json:"timeout_secs"` // default 10
}

// httpFetchResult is written to the output file and returned as the task result.
type httpFetchResult struct {
	URL         string `json:"url"`
	StatusCode  int    `json:"status_code"`
	ContentType string `json:"content_type"`
	BodyBytes   int    `json:"body_bytes"`
	BodyPreview string `json:"body_preview"` // first 500 bytes as UTF-8
	ElapsedMs   int64  `json:"elapsed_ms"`
	OutputFile  string `json:"output_file"`
}

// HTTPFetch fetches a URL and writes a JSON result file to examples/output/.
//
// Payload: {"url": "https://example.com", "timeout_secs": 10}
// Output:  examples/output/go_http_fetch_<timestamp>.json
func HTTPFetch(ctx context.Context, payload []byte) ([]byte, error) {
	var p httpFetchPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("invalid payload: %w", err)
	}
	if p.URL == "" {
		return nil, fmt.Errorf("url is required")
	}
	if p.TimeoutSecs <= 0 {
		p.TimeoutSecs = 10
	}

	client := &http.Client{Timeout: time.Duration(p.TimeoutSecs) * time.Second}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.URL, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("User-Agent", "ErmineTQ-example/0.1")

	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", p.URL, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 64*1024)) // cap at 64 KB
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	elapsed := time.Since(start).Milliseconds()

	preview := string(body)
	if len(preview) > 500 {
		preview = preview[:500] + "…"
	}

	result := httpFetchResult{
		URL:         p.URL,
		StatusCode:  resp.StatusCode,
		ContentType: resp.Header.Get("Content-Type"),
		BodyBytes:   len(body),
		BodyPreview: preview,
		ElapsedMs:   elapsed,
	}

	outPath, err := writeOutputJSON(
		fmt.Sprintf("go_http_fetch_%d.json", time.Now().UnixNano()),
		result,
	)
	if err != nil {
		return nil, err
	}
	result.OutputFile = outPath

	return json.Marshal(result)
}

// ── helpers shared by all handlers ───────────────────────────────────────────

// outputDir returns the path to examples/output/ relative to the working directory.
func outputDir() string {
	return filepath.Join("examples", "output")
}

// writeOutputJSON marshals v as indented JSON and writes it to examples/output/<name>.
// It creates the directory if it does not exist.
func writeOutputJSON(name string, v any) (string, error) {
	if err := os.MkdirAll(outputDir(), 0o755); err != nil {
		return "", fmt.Errorf("mkdir output: %w", err)
	}
	path := filepath.Join(outputDir(), name)
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshal output: %w", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return "", fmt.Errorf("write %s: %w", path, err)
	}
	return path, nil
}

// writeOutputText writes raw text to examples/output/<name>.
func writeOutputText(name, content string) (string, error) {
	if err := os.MkdirAll(outputDir(), 0o755); err != nil {
		return "", fmt.Errorf("mkdir output: %w", err)
	}
	path := filepath.Join(outputDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		return "", fmt.Errorf("write %s: %w", path, err)
	}
	return path, nil
}
