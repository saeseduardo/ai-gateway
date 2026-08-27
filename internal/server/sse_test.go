package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/saeseduardo/ai-gateway/internal/api"
)

// noFlushResponseWriter wraps an http.ResponseWriter through the bare
// http.ResponseWriter interface only, so it does not expose a Flush
// method even when the underlying writer (e.g. httptest.ResponseRecorder)
// has one. It's used to simulate a ResponseWriter that doesn't support
// streaming.
type noFlushResponseWriter struct {
	http.ResponseWriter
}

func TestNewSSEWriter_RequiresFlusher(t *testing.T) {
	w := noFlushResponseWriter{ResponseWriter: httptest.NewRecorder()}

	_, err := newSSEWriter(w)
	if err == nil {
		t.Fatal("newSSEWriter() error = nil, want error for a writer without Flusher support")
	}
}

func TestNewSSEWriter_AcceptsFlusher(t *testing.T) {
	rec := httptest.NewRecorder()

	if _, err := newSSEWriter(rec); err != nil {
		t.Fatalf("newSSEWriter() error = %v, want nil", err)
	}
}

func TestSSEWriter_WriteHeader(t *testing.T) {
	rec := httptest.NewRecorder()
	sw, err := newSSEWriter(rec)
	if err != nil {
		t.Fatalf("newSSEWriter() error = %v", err)
	}

	sw.WriteHeader()

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
	if rec.Code != http.StatusOK {
		t.Errorf("Code = %d, want %d", rec.Code, http.StatusOK)
	}
	if !rec.Flushed {
		t.Error("Flushed = false, want true immediately after WriteHeader")
	}
}

func TestSSEWriter_WriteEvent(t *testing.T) {
	rec := httptest.NewRecorder()
	sw, err := newSSEWriter(rec)
	if err != nil {
		t.Fatalf("newSSEWriter() error = %v", err)
	}

	if err := sw.WriteEvent(map[string]string{"foo": "bar"}); err != nil {
		t.Fatalf("WriteEvent() error = %v, want nil", err)
	}

	want := "data: {\"foo\":\"bar\"}\n\n"
	if got := rec.Body.String(); got != want {
		t.Errorf("WriteEvent() wrote %q, want %q", got, want)
	}
	if !rec.Flushed {
		t.Error("Flushed = false, want true immediately after WriteEvent")
	}
}

// TestSSEWriter_WriteEvent_EscapesMultilineContent covers the
// realistic version of "the payload must not break the SSE frame": an
// assistant reply containing literal newlines (e.g. a code block),
// which is common LLM output, not a hypothetical. It verifies the
// written frame is still exactly one "data: ...\n\n" block -- the
// embedded newlines must come back as escaped "\n" inside the JSON
// string, never as raw bytes that a naive line-based SSE parser (or
// this package's own stream loop) would misread as the end of the
// event.
func TestSSEWriter_WriteEvent_EscapesMultilineContent(t *testing.T) {
	rec := httptest.NewRecorder()
	sw, err := newSSEWriter(rec)
	if err != nil {
		t.Fatalf("newSSEWriter() error = %v", err)
	}

	chunk := api.ChatStreamChunk{
		ID:    "chatcmpl-1",
		Model: "gpt-4o",
		Choices: []api.ChatStreamChoice{
			{Index: 0, Delta: api.ChatStreamDelta{Content: "line one\nline two\r\nline three"}},
		},
	}

	if err := sw.WriteEvent(chunk); err != nil {
		t.Fatalf("WriteEvent() error = %v, want nil", err)
	}

	body := rec.Body.String()
	if !strings.HasSuffix(body, "\n\n") {
		t.Fatalf("body = %q, want it to end with a blank line", body)
	}
	frame := strings.TrimSuffix(body, "\n\n")
	if strings.Contains(frame, "\n") || strings.Contains(frame, "\r") {
		t.Fatalf("frame contains a raw newline, would corrupt SSE framing: %q", frame)
	}

	payload, ok := strings.CutPrefix(frame, "data: ")
	if !ok {
		t.Fatalf("frame missing %q prefix: %q", "data: ", frame)
	}
	var got api.ChatStreamChunk
	if err := json.Unmarshal([]byte(payload), &got); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if got.Choices[0].Delta.Content != chunk.Choices[0].Delta.Content {
		t.Errorf("round-tripped content = %q, want %q", got.Choices[0].Delta.Content, chunk.Choices[0].Delta.Content)
	}
}

func TestSSEWriter_WriteDone(t *testing.T) {
	rec := httptest.NewRecorder()
	sw, err := newSSEWriter(rec)
	if err != nil {
		t.Fatalf("newSSEWriter() error = %v", err)
	}

	if err := sw.WriteDone(); err != nil {
		t.Fatalf("WriteDone() error = %v, want nil", err)
	}

	want := "data: [DONE]\n\n"
	if got := rec.Body.String(); got != want {
		t.Errorf("WriteDone() wrote %q, want %q", got, want)
	}
	if !rec.Flushed {
		t.Error("Flushed = false, want true immediately after WriteDone")
	}
}

func TestSSEWriter_WriteComment(t *testing.T) {
	rec := httptest.NewRecorder()
	sw, err := newSSEWriter(rec)
	if err != nil {
		t.Fatalf("newSSEWriter() error = %v", err)
	}

	if err := sw.WriteComment("keep-alive"); err != nil {
		t.Fatalf("WriteComment() error = %v, want nil", err)
	}

	want := ": keep-alive\n\n"
	if got := rec.Body.String(); got != want {
		t.Errorf("WriteComment() wrote %q, want %q", got, want)
	}
	if !rec.Flushed {
		t.Error("Flushed = false, want true immediately after WriteComment")
	}
}
