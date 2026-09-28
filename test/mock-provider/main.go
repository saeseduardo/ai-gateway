package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

var slowResponse = []string{
	"The ", "history ", "of ", "computing ", "begins ",
	"with ", "mechanical ", "calculators ", "in ", "the ",
	"17th ", "century, ", "but ", "the ", "true ",
	"revolution ", "started ", "with ", "ENIAC ", "in ",
	"1945, ", "the ", "first ", "general-purpose ",
	"electronic ", "computer. ", "From ", "vacuum ",
	"tubes ", "to ", "transistors, ", "from ",
	"mainframes ", "to ", "personal ", "computers, ",
	"the ", "journey ", "has ", "been ", "nothing ",
	"short ", "of ", "extraordinary. ",
}

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatRequest struct {
	Model     string    `json:"model"`
	Messages  []message `json:"messages"`
	MaxTokens int       `json:"max_tokens"`
	Stream    bool      `json:"stream"`
}

func main() {
	http.HandleFunc("/v1/messages", handleMessages)
	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"status":"ok"}`)
	})

	addr := ":8081"
	log.Printf("mock-provider listening on %s", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatal(err)
	}
}

func handleMessages(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Validate headers
	if r.Header.Get("x-api-key") == "" {
		http.Error(w, `{"error":{"type":"authentication_error","message":"missing x-api-key header"}}`, http.StatusUnauthorized)
		return
	}

	var req chatRequest
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, `{"error":{"type":"invalid_request_error","message":"could not read body"}}`, http.StatusBadRequest)
		return
	}
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, `{"error":{"type":"invalid_request_error","message":"invalid JSON"}}`, http.StatusBadRequest)
		return
	}

	log.Printf("request: model=%s stream=%v messages=%d", req.Model, req.Stream, len(req.Messages))

	if !req.Stream {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		resp := fmt.Sprintf(`{"id":"msg_mock","model":"%s","content":[{"type":"text","text":"Hello from mock provider"}],"stop_reason":"end_turn","usage":{"input_tokens":10,"output_tokens":5}}`, req.Model)
		fmt.Fprint(w, resp)
		return
	}

	// Streaming SSE response
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	writer := w

	// message_start
	writeSSEEvent(writer, "message_start", `{"type":"message_start","message":{"id":"msg_mock_stream","type":"message","role":"assistant","content":[],"model":"`+req.Model+`","stop_reason":null,"usage":{"input_tokens":10,"output_tokens":0}}}`)
	flusher.Flush()

	// content_block_start
	writeSSEEvent(writer, "content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`)
	flusher.Flush()

	// Stream tokens slowly - one word every 400ms
	for _, word := range slowResponse {
		select {
		case <-r.Context().Done():
			log.Printf("client disconnected, stopping stream")
			return
		default:
		}

		deltaJSON, _ := json.Marshal(map[string]interface{}{
			"type":  "content_block_delta",
			"index": 0,
			"delta": map[string]string{
				"type": "text_delta",
				"text": word,
			},
		})
		writeSSEEvent(writer, "content_block_delta", string(deltaJSON))
		flusher.Flush()
		time.Sleep(400 * time.Millisecond)
	}

	// content_block_stop
	writeSSEEvent(writer, "content_block_stop", `{"type":"content_block_stop","index":0}`)
	flusher.Flush()

	// message_delta
	writeSSEEvent(writer, "message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":43}}`)
	flusher.Flush()

	// message_stop
	writeSSEEvent(writer, "message_stop", `{"type":"message_stop"}`)
	flusher.Flush()

	log.Printf("stream completed normally")
}

func writeSSEEvent(w io.Writer, eventType, data string) {
	fmt.Fprintf(w, "event: %s\n", eventType)
	fmt.Fprintf(w, "data: %s\n\n", data)
}


