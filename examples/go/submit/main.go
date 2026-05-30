// Command submit submits one example task of each type to the running
// example server and polls until all tasks reach a terminal state.
//
// Prerequisites:
//
//	go run ./examples/go/server   # must be running
//
// Usage:
//
//	go run ./examples/go/submit
//	go run ./examples/go/submit -addr http://localhost:8081
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"time"
)

var baseURL string

func main() {
	flag.StringVar(&baseURL, "addr", "http://localhost:8081", "ErmineTQ server address")
	flag.Parse()

	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

// task definitions: each entry is (task_type, payload_object).
var exampleTasks = []struct {
	Type    string
	Payload any
}{
	{
		Type: "go_http_fetch",
		Payload: map[string]any{
			"url":          "https://httpbin.org/json",
			"timeout_secs": 10,
		},
	},
	{
		Type: "go_file_write",
		Payload: map[string]any{
			"filename": "go_example_write.txt",
			"content":  fmt.Sprintf("Written by ErmineTQ example at %s\n", time.Now().Format(time.RFC3339)),
		},
	},
	{
		Type: "go_file_read",
		Payload: map[string]any{
			"path": "examples/output/go_example_write.txt",
		},
	},
	{
		Type: "go_ollama_chat",
		Payload: map[string]any{
			"model":  "qwen2.5:3b-instruct",
			"prompt": "What's the weather like in Shanghai today? (Answer briefly)",
		},
	},
}

func run() error {
	slog.Info("submitting example tasks", "server", baseURL)

	// Submit all tasks upfront.
	type submitted struct {
		id       string
		taskType string
	}
	var tasks []submitted

	for _, t := range exampleTasks {
		id, err := submitTask(t.Type, t.Payload)
		if err != nil {
			return fmt.Errorf("submit %s: %w", t.Type, err)
		}
		slog.Info("submitted", "type", t.Type, "id", id)
		tasks = append(tasks, submitted{id: id, taskType: t.Type})
	}

	// Poll until all tasks finish (or timeout after 3 minutes).
	slog.Info("waiting for tasks to complete...")
	deadline := time.Now().Add(3 * time.Minute)
	results := make(map[string]map[string]any)

	for len(results) < len(tasks) {
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for tasks")
		}
		time.Sleep(1 * time.Second)

		for _, t := range tasks {
			if _, done := results[t.id]; done {
				continue
			}
			task, err := getTask(t.id)
			if err != nil {
				slog.Warn("poll error", "id", t.id, "err", err)
				continue
			}
			status, _ := task["Status"].(string)
			if isTerminal(status) {
				results[t.id] = task
				slog.Info("completed",
					"type", t.taskType,
					"id", t.id,
					"status", status,
				)
			}
		}
	}

	// Print summary.
	fmt.Println()
	fmt.Println("═══════════════════════════════════════")
	fmt.Println("  Example run complete")
	fmt.Println("═══════════════════════════════════════")
	for _, t := range tasks {
		task := results[t.id]
		fmt.Printf("  %-20s  %s  (id: %s)\n",
			t.taskType, task["Status"], t.id)
	}
	fmt.Println()
	fmt.Printf("  Output files: examples/output/\n")
	fmt.Println("═══════════════════════════════════════")
	return nil
}

// ── HTTP helpers ──────────────────────────────────────────────────────────────

func submitTask(taskType string, payload any) (string, error) {
	body := map[string]any{
		"type":    taskType,
		"payload": payload,
	}
	var task map[string]any
	if err := postJSON("/api/tasks", body, &task); err != nil {
		return "", err
	}
	id, _ := task["ID"].(string)
	if id == "" {
		return "", fmt.Errorf("server returned empty task ID")
	}
	return id, nil
}

func getTask(id string) (map[string]any, error) {
	var resp map[string]any
	if err := getJSON("/api/tasks/"+id, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func postJSON(path string, body, out any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	resp, err := http.Post(baseURL+path, "application/json", bytes.NewReader(b))
	if err != nil {
		return fmt.Errorf("POST %s: %w", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		raw, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("POST %s: HTTP %d: %s", path, resp.StatusCode, raw)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func getJSON(path string, out any) error {
	resp, err := http.Get(baseURL + path)
	if err != nil {
		return fmt.Errorf("GET %s: %w", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		raw, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("GET %s: HTTP %d: %s", path, resp.StatusCode, raw)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func isTerminal(status string) bool {
	switch status {
	case "succeeded", "dead", "cancelled":
		return true
	}
	return false
}
