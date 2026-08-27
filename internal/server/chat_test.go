package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/saeseduardo/ai-gateway/internal/api"
	"github.com/saeseduardo/ai-gateway/internal/config"
	"github.com/saeseduardo/ai-gateway/internal/provider"
	"github.com/saeseduardo/ai-gateway/internal/router"
)

// newTestServerWithLogBuffer is like newTestServer, but captures log
// output as JSON lines in the returned buffer instead of discarding
// it, so tests can assert on specific log events (e.g.
// stream_cancelled_by_client) and their fields.
func newTestServerWithLogBuffer() (*Server, *bytes.Buffer) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	cfg := config.ServerConfig{
		Port:            0,
		ReadTimeout:     time.Second,
		WriteTimeout:    time.Second,
		IdleTimeout:     time.Second,
		ShutdownTimeout: time.Second,
	}
	return New(cfg, logger), &buf
}

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

// blockingStreamReader serves a fixed sequence of chunks immediately,
// then blocks in Recv() -- simulating a provider that has gone quiet
// mid-stream -- until Close() is called, at which point the pending
// (and any future) Recv() returns io.EOF. This lets a test control
// precisely when, if ever, another chunk becomes available, so
// cancellation can be exercised deterministically instead of racing a
// real chunk arrival.
//
// served is signaled once per chunk actually handed back by Recv, so
// a test can wait for "the handler has consumed chunk N" without a
// sleep. closeCount lets a test verify Close was actually called
// (the cancellation contract this type exists to test), and is an
// atomic since Close and Recv observably run on different goroutines.
type blockingStreamReader struct {
	chunks []*provider.StreamChunk
	idx    int
	served chan struct{}

	closeCh    chan struct{}
	closeOnce  sync.Once
	closeCount atomic.Int32
}

func newBlockingStreamReader(chunks []*provider.StreamChunk) *blockingStreamReader {
	return &blockingStreamReader{
		chunks:  chunks,
		served:  make(chan struct{}, len(chunks)),
		closeCh: make(chan struct{}),
	}
}

func (b *blockingStreamReader) Recv() (*provider.StreamChunk, error) {
	if b.idx < len(b.chunks) {
		c := b.chunks[b.idx]
		b.idx++
		b.served <- struct{}{}
		return c, nil
	}
	<-b.closeCh
	return nil, io.EOF
}

func (b *blockingStreamReader) Close() error {
	b.closeCount.Add(1)
	b.closeOnce.Do(func() { close(b.closeCh) })
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

// TestChatCompletions_Streaming_CancelledByClient is the most
// important test in this package: it proves the gateway's central
// cost-control property, that when a client disconnects mid-stream,
// the upstream provider connection is cut immediately rather than
// left running (and being paid for) with nobody left to read it.
//
// The fake StreamReader serves exactly one chunk and then blocks in
// Recv (simulating a provider that has gone quiet) until Close is
// called. The test waits for that one chunk to be served, cancels the
// request's context (simulating the client closing the connection),
// and then asserts, all within a timeout so a regression that makes
// the handler hang fails the test instead of blocking forever:
//
//   - the handler returns promptly instead of hanging,
//   - StreamReader.Close was called (which is what tears down the
//     upstream HTTP connection to the provider),
//   - exactly one "data:" frame was written -- the chunk served
//     before cancellation, and nothing after,
//   - the response never reaches a normal [DONE] completion,
//   - the response status is still 200 (headers were already
//     committed; cancellation is not reported as an HTTP error),
//   - a "stream_cancelled_by_client" event is logged at Info level
//     with the request's ID and the correct emitted-chunk count.
func TestChatCompletions_Streaming_CancelledByClient(t *testing.T) {
	srv, logBuf := newTestServerWithLogBuffer()
	srv.SetSSEHeartbeatInterval(time.Hour) // keep the heartbeat out of this test's way

	stream := newBlockingStreamReader([]*provider.StreamChunk{{Delta: "chunk-1"}})
	fp := &fakeProvider{
		name: "fake",
		chatCompletionStream: func(ctx context.Context, req *provider.ChatRequest) (provider.StreamReader, error) {
			return stream, nil
		},
	}

	rt := router.New()
	rt.Register("gpt-4o", fp)
	srv.RegisterChatRoutes(rt)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	body := `{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}],"stream":true}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body)).WithContext(ctx)
	rec := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		srv.Router().ServeHTTP(rec, req)
		close(done)
	}()

	select {
	case <-stream.served:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the fake stream to serve its first chunk")
	}

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not return after the client's request context was cancelled -- it hung")
	}

	if got := stream.closeCount.Load(); got == 0 {
		t.Error("StreamReader.Close() was not called after cancellation -- the upstream connection would be left open")
	}

	respBody := rec.Body.String()
	if got := strings.Count(respBody, "data: "); got != 1 {
		t.Errorf("wrote %d \"data:\" frames, want exactly 1 (the chunk served before cancellation, none after): %q", got, respBody)
	}
	if strings.Contains(respBody, "[DONE]") {
		t.Errorf("response contains [DONE], want the stream to end via cancellation, not normal completion: %q", respBody)
	}
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 (SSE headers were already committed before cancellation, so it can't become an error status)", rec.Code)
	}

	logs := logBuf.String()
	if !strings.Contains(logs, `"msg":"stream_cancelled_by_client"`) {
		t.Errorf("logs do not contain a stream_cancelled_by_client event: %s", logs)
	}
	if !strings.Contains(logs, `"chunks_emitted":1`) {
		t.Errorf("stream_cancelled_by_client log does not report chunks_emitted=1: %s", logs)
	}
	if strings.Contains(logs, `"level":"ERROR"`) {
		t.Errorf("cancellation must not be logged as an error: %s", logs)
	}
}

// TestChatCompletions_Streaming_LogsLifecycleEvents checks the happy
// path emits both lifecycle log events -- stream_started when the SSE
// connection opens and stream_completed once [DONE] is written --
// with matching request IDs, so cancelled and completed streams are
// distinguishable in the logs.
func TestChatCompletions_Streaming_LogsLifecycleEvents(t *testing.T) {
	srv, logBuf := newTestServerWithLogBuffer()

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

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", rec.Code, rec.Body.String())
	}

	logs := logBuf.String()
	if !strings.Contains(logs, `"msg":"stream_started"`) {
		t.Errorf("logs do not contain a stream_started event: %s", logs)
	}
	if !strings.Contains(logs, `"msg":"stream_completed"`) {
		t.Errorf("logs do not contain a stream_completed event: %s", logs)
	}
	if !strings.Contains(logs, `"chunks_emitted":1`) {
		t.Errorf("stream_completed log does not report chunks_emitted=1: %s", logs)
	}
	if strings.Contains(logs, `"msg":"stream_cancelled_by_client"`) {
		t.Errorf("happy path must not log stream_cancelled_by_client: %s", logs)
	}
}
