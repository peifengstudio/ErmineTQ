package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

const (
	ollamaBaseURL      = "http://localhost:11434"
	defaultOllamaModel = "qwen2.5:3b-instruct"
)

// ── /api/chat wire types ──────────────────────────────────────────────────────

type ollamaChatPayload struct {
	Model  string `json:"model"`  // default: qwen2.5:3b-instruct
	Prompt string `json:"prompt"` // user message content
}

type ollamaChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// ollamaChatRequest is the body sent to POST /api/chat.
type ollamaChatRequest struct {
	Model    string              `json:"model"`
	Messages []ollamaChatMessage `json:"messages"`
	Stream   bool                `json:"stream"` // always false
}

// ollamaChatAPIResponse is the relevant subset of Ollama's /api/chat response.
type ollamaChatAPIResponse struct {
	Model   string            `json:"model"`
	Message ollamaChatMessage `json:"message"`
	Done    bool              `json:"done"`
}

// ollamaChatResult is written to the output file and returned as the task result.
type ollamaChatResult struct {
	Model           string `json:"model"`
	Prompt          string `json:"prompt"`
	Response        string `json:"response"`
	ResponsePreview string `json:"response_preview"`
	ElapsedMs       int64  `json:"elapsed_ms"`
	OutputFile      string `json:"output_file"`
}

// OllamaChat sends a user message to a locally running Ollama instance via
// /api/chat and writes the response to examples/output/.
//
// Payload:  {"model": "qwen2.5:3b-instruct", "prompt": "What's the weather like today?"}
// Output:   examples/output/go_ollama_<timestamp>.txt
//
// If Ollama is not running the task returns an error, triggering normal
// retry / dead logic. Start Ollama with: ollama serve
func OllamaChat(ctx context.Context, payload []byte) ([]byte, error) {
	var p ollamaChatPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("invalid payload: %w", err)
	}
	if p.Prompt == "" {
		return nil, fmt.Errorf("prompt is required")
	}
	if p.Model == "" {
		p.Model = defaultOllamaModel
	}

	reqBody, err := json.Marshal(ollamaChatRequest{
		Model: p.Model,
		Messages: []ollamaChatMessage{
			{Role: "user", Content: p.Prompt},
		},
		Stream: false,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		ollamaBaseURL+"/api/chat", bytes.NewReader(reqBody))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 2 * time.Minute}
	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf(
			"Ollama unavailable at %s (run: ollama serve): %w",
			ollamaBaseURL, err,
		)
	}
	defer resp.Body.Close()
	elapsed := time.Since(start).Milliseconds()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Ollama returned HTTP %d", resp.StatusCode)
	}

	var ollamaResp ollamaChatAPIResponse
	if err := json.NewDecoder(resp.Body).Decode(&ollamaResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	responseText := ollamaResp.Message.Content
	preview := responseText
	if len(preview) > 200 {
		preview = preview[:200] + "…"
	}

	logContent := fmt.Sprintf(
		"=== go_ollama_chat ===\nmodel:      %s\nprompt:     %s\nelapsed_ms: %d\n\n--- response ---\n%s\n",
		p.Model, p.Prompt, elapsed, responseText,
	)
	outPath, err := writeOutputText(
		fmt.Sprintf("go_ollama_%d.txt", time.Now().UnixNano()),
		logContent,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(ollamaChatResult{
		Model:           ollamaResp.Model,
		Prompt:          p.Prompt,
		Response:        responseText,
		ResponsePreview: preview,
		ElapsedMs:       elapsed,
		OutputFile:      outPath,
	})
}
