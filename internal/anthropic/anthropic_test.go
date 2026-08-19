package anthropic

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/saeseduardo/ai-gateway/internal/config"
	"github.com/saeseduardo/ai-gateway/internal/provider"
)

func testConfig(baseURL string) config.ProviderConfig {
	return config.ProviderConfig{
		APIKey:  "test-key",
		BaseURL: baseURL,
		Timeout: 5 * time.Second,
	}
}

func testRequest() *provider.ChatRequest {
	return &provider.ChatRequest{
		Model: "claude-sonnet-5",
		Messages: []provider.Message{
			{Role: provider.RoleUser, Content: "hi"},
		},
		MaxTokens: 100,
	}
}

func writeEvent(w http.ResponseWriter, flusher http.Flusher, event, data string) {
	fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, data)
	flusher.Flush()
}

func TestChatCompletion_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("x-api-key"); got != "test-key" {
			t.Errorf("unexpected x-api-key header: %q", got)
		}
		if got := r.Header.Get("anthropic-version"); got != apiVersion {
			t.Errorf("unexpected anthropic-version header: %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"id":"msg_1","model":"claude-sonnet-5","content":[{"type":"text","text":"hello"}],"stop_reason":"end_turn","usage":{"input_tokens":5,"output_tokens":2}}`)
	}))
	defer srv.Close()

	p := New(testConfig(srv.URL))
	resp, err := p.ChatCompletion(context.Background(), testRequest())
	if err != nil {
		t.Fatalf("ChatCompletion: %v", err)
	}
	if resp.Choices[0].Message.Content != "hello" {
		t.Errorf("Content = %q, want %q", resp.Choices[0].Message.Content, "hello")
	}
	if resp.Usage.TotalTokens != 7 {
		t.Errorf("TotalTokens = %d, want 7", resp.Usage.TotalTokens)
	}
}

func TestChatCompletion_RateLimited(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		fmt.Fprint(w, `{"type":"error","error":{"type":"rate_limit_error","message":"rate limited"}}`)
	}))
	defer srv.Close()

	p := New(testConfig(srv.URL))
	_, err := p.ChatCompletion(context.Background(), testRequest())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var perr *provider.ProviderError
	if !errors.As(err, &perr) {
		t.Fatalf("expected *provider.ProviderError, got %T: %v", err, err)
	}
	if !errors.Is(err, provider.ErrRateLimited) {
		t.Errorf("expected errors.Is(err, ErrRateLimited)")
	}
	if !perr.Retryable {
		t.Errorf("expected Retryable = true")
	}
}

func TestChatCompletionStream_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher := w.(http.Flusher)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)

		writeEvent(w, flusher, "message_start", `{"type":"message_start","message":{"id":"msg_1","model":"claude-sonnet-5","usage":{"input_tokens":10,"output_tokens":1}}}`)
		writeEvent(w, flusher, "content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`)
		writeEvent(w, flusher, "ping", `{"type":"ping"}`)
		writeEvent(w, flusher, "content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hello"}}`)
		writeEvent(w, flusher, "content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":" world"}}`)
		writeEvent(w, flusher, "content_block_stop", `{"type":"content_block_stop","index":0}`)
		writeEvent(w, flusher, "message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":5}}`)
		writeEvent(w, flusher, "message_stop", `{"type":"message_stop"}`)
	}))
	defer srv.Close()

	p := New(testConfig(srv.URL))
	req := testRequest()
	req.Stream = true

	stream, err := p.ChatCompletionStream(context.Background(), req)
	if err != nil {
		t.Fatalf("ChatCompletionStream: %v", err)
	}
	defer stream.Close()

	var text string
	var lastFinish string
	var usage *provider.Usage
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		text += chunk.Delta
		if chunk.FinishReason != "" {
			lastFinish = chunk.FinishReason
		}
		if chunk.Usage != nil {
			usage = chunk.Usage
		}
	}

	if text != "Hello world" {
		t.Errorf("text = %q, want %q", text, "Hello world")
	}
	if lastFinish != "end_turn" {
		t.Errorf("FinishReason = %q, want %q", lastFinish, "end_turn")
	}
	if usage == nil {
		t.Fatal("expected non-nil usage")
	}
	if usage.PromptTokens != 10 {
		t.Errorf("PromptTokens = %d, want 10", usage.PromptTokens)
	}
	if usage.CompletionTokens != 5 {
		t.Errorf("CompletionTokens = %d, want 5", usage.CompletionTokens)
	}
	if usage.TotalTokens != 15 {
		t.Errorf("TotalTokens = %d, want 15", usage.TotalTokens)
	}

	// Further Recv calls after EOF must keep returning EOF.
	if _, err := stream.Recv(); err != io.EOF {
		t.Errorf("Recv after EOF = %v, want io.EOF", err)
	}

	// Close must be idempotent.
	if err := stream.Close(); err != nil {
		t.Errorf("first Close: %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

func TestChatCompletionStream_RateLimited(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		fmt.Fprint(w, `{"type":"error","error":{"type":"rate_limit_error","message":"rate limited"}}`)
	}))
	defer srv.Close()

	p := New(testConfig(srv.URL))
	req := testRequest()
	req.Stream = true

	stream, err := p.ChatCompletionStream(context.Background(), req)
	if stream != nil {
		t.Errorf("expected nil stream, got %v", stream)
	}
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var perr *provider.ProviderError
	if !errors.As(err, &perr) {
		t.Fatalf("expected *provider.ProviderError, got %T: %v", err, err)
	}
	if !errors.Is(err, provider.ErrRateLimited) {
		t.Errorf("expected errors.Is(err, ErrRateLimited)")
	}
	if !perr.Retryable {
		t.Errorf("expected Retryable = true")
	}
}

func TestChatCompletionStream_Cancellation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher := w.(http.Flusher)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)

		writeEvent(w, flusher, "message_start", `{"type":"message_start","message":{"id":"msg_1","model":"claude-sonnet-5","usage":{"input_tokens":10,"output_tokens":1}}}`)
		writeEvent(w, flusher, "content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hello"}}`)

		// Hold the connection open until the client cancels (which
		// tears down the connection and cancels this request's
		// context) rather than sending more data or closing cleanly.
		<-r.Context().Done()
	}))
	defer srv.Close()

	p := New(testConfig(srv.URL))
	req := testRequest()
	req.Stream = true

	ctx, cancel := context.WithCancel(context.Background())
	stream, err := p.ChatCompletionStream(ctx, req)
	if err != nil {
		t.Fatalf("ChatCompletionStream: %v", err)
	}
	defer stream.Close()

	chunk, err := stream.Recv()
	if err != nil {
		t.Fatalf("first Recv: %v", err)
	}
	if chunk.Delta != "Hello" {
		t.Fatalf("first Delta = %q, want %q", chunk.Delta, "Hello")
	}

	cancel()

	_, err = stream.Recv()
	if err == nil {
		t.Fatal("expected error after cancellation, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected errors.Is(err, context.Canceled), got %v", err)
	}

	// The error must stick on subsequent calls too.
	if _, err := stream.Recv(); !errors.Is(err, context.Canceled) {
		t.Errorf("second Recv after cancellation = %v, want errors.Is context.Canceled", err)
	}
}
