package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

// defaultSSEHeartbeatInterval is how long an SSE stream can go without
// a chunk before a keep-alive heartbeat is sent, when the server
// hasn't been configured with a different interval via
// Server.SetSSEHeartbeatInterval.
const defaultSSEHeartbeatInterval = 15 * time.Second

// sseWriter writes a Server-Sent Events response to an underlying
// http.ResponseWriter. Every write flushes immediately, so the client
// receives each event as soon as it's produced instead of it sitting
// in Go's or a proxy's internal buffers until the handler returns.
type sseWriter struct {
	w       http.ResponseWriter
	flusher http.Flusher
}

// newSSEWriter wraps w for SSE output. It fails if w does not
// implement http.Flusher: without Flush, nothing written to w would
// reach the client until the handler returns, defeating streaming
// entirely, so callers should fail the request early instead of
// silently buffering it.
func newSSEWriter(w http.ResponseWriter) (*sseWriter, error) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		return nil, errors.New("response writer does not support flushing, streaming is unavailable")
	}
	return &sseWriter{w: w, flusher: flusher}, nil
}

// WriteHeader sets the SSE response headers, writes the 200 status,
// and flushes immediately -- establishing the connection before the
// first event is produced, so the client knows the stream has opened
// even if the first chunk takes a while to arrive.
func (sw *sseWriter) WriteHeader() {
	h := sw.w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	// Reverse proxies (nginx and similar) buffer proxied responses by
	// default, holding each SSE frame until the buffer fills or the
	// response ends -- which would defeat streaming in production even
	// though it works fine against the Go server directly. This header
	// is nginx's documented opt-out (see the proxy_buffering
	// directive) and is harmless to send to proxies that don't
	// recognize it.
	h.Set("X-Accel-Buffering", "no")
	sw.w.WriteHeader(http.StatusOK)
	sw.flusher.Flush()
}

// WriteEvent marshals v as JSON and writes it as one SSE "data:"
// frame, then flushes. Per the SSE spec a frame ends at the first
// blank line, so v's JSON encoding must not contain a raw newline --
// that would be read as the end of the frame partway through the
// payload (e.g. multi-line assistant output), corrupting the stream.
// This holds unconditionally for any v marshaled through
// encoding/json: newlines inside string values are always escaped to
// "\n", and even a custom MarshalJSON's output is compacted (stripped
// of insignificant whitespace) by the encoder before use -- see
// TestSSEWriter_WriteEvent_EscapesMultilineContent.
func (sw *sseWriter) WriteEvent(v any) error {
	payload, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("sse: marshal event: %w", err)
	}
	if _, err := fmt.Fprintf(sw.w, "data: %s\n\n", payload); err != nil {
		return err
	}
	sw.flusher.Flush()
	return nil
}

// WriteDone writes the "[DONE]" sentinel frame that signals the end
// of the stream, matching OpenAI's wire format.
func (sw *sseWriter) WriteDone() error {
	if _, err := io.WriteString(sw.w, "data: [DONE]\n\n"); err != nil {
		return err
	}
	sw.flusher.Flush()
	return nil
}

// WriteComment writes an SSE comment line (starting with ":"), which
// the SSE spec has clients ignore as content but which still counts
// as activity on the connection. It's used here as a keep-alive
// heartbeat so idle proxies, load balancers, or the client's own idle
// timeout don't close the connection while waiting on a slow upstream
// provider between chunks.
func (sw *sseWriter) WriteComment(text string) error {
	if _, err := fmt.Fprintf(sw.w, ": %s\n\n", text); err != nil {
		return err
	}
	sw.flusher.Flush()
	return nil
}
