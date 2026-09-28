package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
)

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	gatewayURL := getenv("GATEWAY_URL", "http://gateway:8080")
	model := getenv("GATEWAY_MODEL", "claude-sonnet-5")
	maxChunks := 4

	fmt.Println("=== E2E: client-side streaming cancellation ===")
	fmt.Printf("POST  %s/v1/chat/completions\n", gatewayURL)
	fmt.Printf("      model=%s  stream=true\n\n", model)

	payload := fmt.Sprintf(
		`{"model":%q,"messages":[{"role":"user","content":"Write a very long essay about the history of computing"}],"stream":true}`,
		model,
	)

	req, err := http.NewRequest(http.MethodPost, gatewayURL+"/v1/chat/completions", bytes.NewBufferString(payload))
	if err != nil {
		fmt.Fprintln(os.Stderr, "build request:", err)
		os.Exit(1)
	}
	req.Header.Set("Content-Type", "application/json")

	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Fprintln(os.Stderr, "request failed:", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	fmt.Printf("HTTP %d  ->  SSE stream open\n\n", resp.StatusCode)

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	chunks := 0
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		data := strings.TrimPrefix(line, "data: ")
		if data == "[DONE]" {
			fmt.Println("[DONE]  ->  stream completed normally")
			return
		}

		var chunk struct {
			Choices []struct {
				Delta struct {
					Content string `json:"content"`
				} `json:"delta"`
			} `json:"choices"`
		}
		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			continue
		}
		if len(chunk.Choices) == 0 || chunk.Choices[0].Delta.Content == "" {
			continue
		}

		chunks++
		el := time.Since(start).Seconds()
		fmt.Printf("  [%4.1fs] chunk %d:  %q\n", el, chunks, chunk.Choices[0].Delta.Content)

		if chunks >= maxChunks {
			break
		}
	}

	total := time.Since(start).Seconds()
	fmt.Printf("\n>>> client disconnects after %d chunks (%.1fs) <<<\n", chunks, total)
	fmt.Println(">>> the gateway should now log: stream_cancelled_by_client")
}