package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/saeseduardo/ai-gateway/internal/api"
	"github.com/saeseduardo/ai-gateway/internal/provider"
	"github.com/saeseduardo/ai-gateway/internal/router"
)

// fakeProvider is a provider.Provider stub whose behavior is
// configured per test via its function fields.
type fakeProvider struct {
	name                 string
	chatCompletion       func(ctx context.Context, req *provider.ChatRequest) (*provider.ChatResponse, error)
	chatCompletionStream func(ctx context.Context, req *provider.ChatRequest) (provider.StreamReader, error)
}

func (f *fakeProvider) Name() string { return f.name }

func (f *fakeProvider) ChatCompletion(ctx context.Context, req *provider.ChatRequest) (*provider.ChatResponse, error) {
	return f.chatCompletion(ctx, req)
}

func (f *fakeProvider) ChatCompletionStream(ctx context.Context, req *provider.ChatRequest) (provider.StreamReader, error) {
	return f.chatCompletionStream(ctx, req)
}

// fakeStreamReader replays a fixed sequence of chunks, then io.EOF.
type fakeStreamReader struct {
	chunks []*provider.StreamChunk
	idx    int
	closed bool
}

func (f *fakeStreamReader) Recv() (*provider.StreamChunk, error) {
	if f.idx >= len(f.chunks) {
		return nil, io.EOF
	}
	c := f.chunks[f.idx]
	f.idx++
	return c, nil
}

func (f *fakeStreamReader) Close() error {
	f.closed = true
	return nil
}

// slowStreamReader replays chunks like fakeStreamReader, but sleeps
// for delays[i] (if present) before returning chunks[i], letting tests
// simulate a provider that is slow to produce a given chunk.
type slowStreamReader struct {
	chunks []*provider.StreamChunk
	delays []time.Duration
	idx    int
	closed bool
}

func (s *slowStreamReader) Recv() (*provider.StreamChunk, error) {
	if s.idx < len(s.delays) {
		time.Sleep(s.delays[s.idx])
	}
	if s.idx >= len(s.chunks) {
		return nil, io.EOF
	}
	c := s.chunks[s.idx]
	s.idx++
	return c, nil
}

func (s *slowStreamReader) Close() error {
	s.closed = true
	return nil
}

func TestChatCompletions_NonStreaming(t *testing.T) {
	srv := newTestServer()

	fp := &fakeProvider{
		name: "fake",
		chatCompletion: func(ctx context.Context, req *provider.ChatRequest) (*provider.ChatResponse, error) {
			if req.Model != "gpt-4o" {
				t.Errorf("provider saw Model = %q, want gpt-4o", req.Model)
			}
			if len(req.Messages) != 1 || req.Messages[0].Content != "hi" {
				t.Errorf("provider saw Messages = %+v", req.Messages)
			}
			return &provider.ChatResponse{
				ID:    "resp-1",
				Model: "gpt-4o",
				Choices: []provider.Choice{
					{
						Index:        0,
						Message:      provider.Message{Role: provider.RoleAssistant, Content: "hi there"},
						FinishReason: "stop",
					},
				},
				Usage: provider.Usage{PromptTokens: 3, CompletionTokens: 2, TotalTokens: 5},
			}, nil
		},
	}

	rt := router.New()
	rt.Register("gpt-4o", fp)
	srv.RegisterChatRoutes(rt)

	body := `{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}],"stream":false}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	rec := httptest.NewRecorder()

	srv.Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", rec.Code, rec.Body.String())
	}

	var resp api.ChatResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp.Object != "chat.completion" {
		t.Errorf("Object = %q, want %q", resp.Object, "chat.completion")
	}
	if len(resp.Choices) != 1 || resp.Choices[0].Message.Content != "hi there" {
		t.Errorf("Choices = %+v", resp.Choices)
	}
	if resp.Choices[0].FinishReason != "stop" {
		t.Errorf("FinishReason = %q, want %q", resp.Choices[0].FinishReason, "stop")
	}
	if resp.Usage.TotalTokens != 5 {
		t.Errorf("TotalTokens = %d, want 5", resp.Usage.TotalTokens)
	}
}

func TestChatCompletions_Streaming(t *testing.T) {
	srv := newTestServer()

	stream := &fakeStreamReader{
		chunks: []*provider.StreamChunk{
			{Delta: "Hel"},
			{Delta: "lo "},
			{Delta: "world", FinishReason: "stop"},
		},
	}

	fp := &fakeProvider{
		name: "fake",
		chatCompletionStream: func(ctx context.Context, req *provider.ChatRequest) (provider.StreamReader, error) {
			if !req.Stream {
				t.Errorf("provider saw Stream = false, want true")
			}
			return stream, nil
		},
	}

	rt := router.New()
	rt.Register("gpt-4o", fp)
	srv.RegisterChatRoutes(rt)

	body := `{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}],"stream":true}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	rec := httptest.NewRecorder()

	srv.Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("Content-Type = %q, want %q", ct, "text/event-stream")
	}
	if !stream.closed {
		t.Errorf("StreamReader was not closed")
	}

	raw := strings.TrimRight(rec.Body.String(), "\n")
	var dataLines []string
	for _, block := range strings.Split(raw, "\n\n") {
		if strings.TrimSpace(block) != "" {
			dataLines = append(dataLines, block)
		}
	}
	if len(dataLines) != 4 {
		t.Fatalf("got %d SSE lines, want 4 (3 chunks + DONE): %v", len(dataLines), dataLines)
	}

	var text string
	for _, line := range dataLines[:3] {
		payload, ok := strings.CutPrefix(line, "data: ")
		if !ok {
			t.Fatalf("line missing data: prefix: %q", line)
		}
		var chunk api.ChatStreamChunk
		if err := json.Unmarshal([]byte(payload), &chunk); err != nil {
			t.Fatalf("unmarshal chunk %q: %v", payload, err)
		}
		if chunk.Model != "gpt-4o" {
			t.Errorf("chunk.Model = %q, want gpt-4o", chunk.Model)
		}
		text += chunk.Choices[0].Delta.Content
	}
	if text != "Hello world" {
		t.Errorf("concatenated deltas = %q, want %q", text, "Hello world")
	}

	if dataLines[3] != "data: [DONE]" {
		t.Errorf("last SSE line = %q, want %q", dataLines[3], "data: [DONE]")
	}
}

func TestChatCompletions_MissingModel(t *testing.T) {
	srv := newTestServer()
	srv.RegisterChatRoutes(router.New())

	body := `{"messages":[{"role":"user","content":"hi"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	rec := httptest.NewRecorder()

	srv.Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400, body=%s", rec.Code, rec.Body.String())
	}

	var errBody errorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &errBody); err != nil {
		t.Fatalf("unmarshal error body: %v", err)
	}
	if errBody.Error.Type != "invalid_request" {
		t.Errorf("error.type = %q, want %q", errBody.Error.Type, "invalid_request")
	}
}

func TestChatCompletions_MissingMessages(t *testing.T) {
	srv := newTestServer()
	srv.RegisterChatRoutes(router.New())

	body := `{"model":"gpt-4o","messages":[]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	rec := httptest.NewRecorder()

	srv.Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400, body=%s", rec.Code, rec.Body.String())
	}
}

func TestChatCompletions_UnknownModel(t *testing.T) {
	srv := newTestServer()
	srv.RegisterChatRoutes(router.New())

	body := `{"model":"unknown-model","messages":[{"role":"user","content":"hi"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	rec := httptest.NewRecorder()

	srv.Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404, body=%s", rec.Code, rec.Body.String())
	}

	var errBody errorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &errBody); err != nil {
		t.Fatalf("unmarshal error body: %v", err)
	}
	if errBody.Error.Type != "model_not_found" {
		t.Errorf("error.type = %q, want %q", errBody.Error.Type, "model_not_found")
	}
}

func TestChatCompletions_ProviderRateLimited(t *testing.T) {
	srv := newTestServer()

	fp := &fakeProvider{
		name: "fake",
		chatCompletion: func(ctx context.Context, req *provider.ChatRequest) (*provider.ChatResponse, error) {
			return nil, &provider.ProviderError{
				Provider:   "fake",
				StatusCode: http.StatusTooManyRequests,
				Retryable:  true,
				Err:        provider.ErrRateLimited,
			}
		},
	}

	rt := router.New()
	rt.Register("gpt-4o", fp)
	srv.RegisterChatRoutes(rt)

	body := `{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	rec := httptest.NewRecorder()

	srv.Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429, body=%s", rec.Code, rec.Body.String())
	}
}

func TestChatCompletions_Streaming_ResponseWriterWithoutFlusher(t *testing.T) {
	srv := newTestServer()

	fp := &fakeProvider{
		name: "fake",
		chatCompletionStream: func(ctx context.Context, req *provider.ChatRequest) (provider.StreamReader, error) {
			t.Fatal("provider should not be called once the writer is known not to support flushing")
			return nil, nil
		},
	}

	rt := router.New()
	rt.Register("gpt-4o", fp)
	srv.RegisterChatRoutes(rt)

	body := `{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}],"stream":true}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	rec := httptest.NewRecorder()
	w := noFlushResponseWriter{ResponseWriter: rec}

	srv.Router().ServeHTTP(w, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500, body=%s", rec.Code, rec.Body.String())
	}

	var errBody errorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &errBody); err != nil {
		t.Fatalf("unmarshal error body: %v", err)
	}
	if errBody.Error.Type != "internal_error" {
		t.Errorf("error.type = %q, want %q", errBody.Error.Type, "internal_error")
	}
}

func TestChatCompletions_Streaming_SSEHeaders(t *testing.T) {
	srv := newTestServer()

	stream := &fakeStreamReader{
		chunks: []*provider.StreamChunk{{Delta: "hi", FinishReason: "stop"}},
	}
	fp := &fakeProvider{
		name: "fake",
		chatCompletionStream: func(ctx context.Context, req *provider.ChatRequest) (provider.StreamReader, error) {
			return stream, nil
		},
	}

	rt := router.New()
	rt.Register("gpt-4o", fp)
	srv.RegisterChatRoutes(rt)

	body := `{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}],"stream":true}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	rec := httptest.NewRecorder()

	srv.Router().ServeHTTP(rec, req)

	if got := rec.Header().Get("Content-Type"); got != "text/event-stream" {
		t.Errorf("Content-Type = %q, want %q", got, "text/event-stream")
	}
	if got := rec.Header().Get("Cache-Control"); got != "no-cache" {
		t.Errorf("Cache-Control = %q, want %q", got, "no-cache")
	}
	if got := rec.Header().Get("Connection"); got != "keep-alive" {
		t.Errorf("Connection = %q, want %q", got, "keep-alive")
	}
	if got := rec.Header().Get("X-Accel-Buffering"); got != "no" {
		t.Errorf("X-Accel-Buffering = %q, want %q", got, "no")
	}
}

// TestChatCompletions_Streaming_Heartbeat simulates a provider that
// takes a while to produce its first chunk, with a short heartbeat
// interval configured, and verifies at least one SSE comment
// (": keep-alive") is written before the first data frame -- proving
// the connection stays active instead of sitting silent (and at risk
// of an idle timeout) while waiting on a slow upstream.
func TestChatCompletions_Streaming_Heartbeat(t *testing.T) {
	srv := newTestServer()
	srv.SetSSEHeartbeatInterval(10 * time.Millisecond)

	stream := &slowStreamReader{
		chunks: []*provider.StreamChunk{{Delta: "hi", FinishReason: "stop"}},
		delays: []time.Duration{60 * time.Millisecond},
	}
	fp := &fakeProvider{
		name: "fake",
		chatCompletionStream: func(ctx context.Context, req *provider.ChatRequest) (provider.StreamReader, error) {
			return stream, nil
		},
	}

	rt := router.New()
	rt.Register("gpt-4o", fp)
	srv.RegisterChatRoutes(rt)

	body := `{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}],"stream":true}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	rec := httptest.NewRecorder()

	srv.Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", rec.Code, rec.Body.String())
	}

	raw := strings.TrimRight(rec.Body.String(), "\n")
	var blocks []string
	for _, b := range strings.Split(raw, "\n\n") {
		if strings.TrimSpace(b) != "" {
			blocks = append(blocks, b)
		}
	}

	sawHeartbeatBeforeData := false
	for _, b := range blocks {
		if strings.HasPrefix(b, ": ") {
			sawHeartbeatBeforeData = true
			continue
		}
		if strings.HasPrefix(b, "data: ") {
			break
		}
	}
	if !sawHeartbeatBeforeData {
		t.Fatalf("no heartbeat comment line found before the first data frame: %q", raw)
	}
	if !stream.closed {
		t.Error("StreamReader was not closed")
	}
}
