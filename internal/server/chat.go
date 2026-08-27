package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5/middleware"

	"github.com/saeseduardo/ai-gateway/internal/api"
	"github.com/saeseduardo/ai-gateway/internal/provider"
	"github.com/saeseduardo/ai-gateway/internal/router"
)

// RegisterChatRoutes mounts the chat completions endpoint on s's
// router, resolving each request's model through rt.
func (s *Server) RegisterChatRoutes(rt *router.Router) {
	s.router.Post("/v1/chat/completions", s.handleChatCompletions(rt))
}

// handleChatCompletions decodes and validates the request body,
// resolves its model to a provider.Provider via rt, and dispatches to
// either the non-streaming or SSE streaming path based on
// api.ChatRequest.Stream.
func (s *Server) handleChatCompletions(rt *router.Router) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req api.ChatRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON body", "invalid_request")
			return
		}
		if req.Model == "" {
			writeError(w, http.StatusBadRequest, "model is required", "invalid_request")
			return
		}
		if len(req.Messages) == 0 {
			writeError(w, http.StatusBadRequest, "messages must not be empty", "invalid_request")
			return
		}

		p, err := rt.Resolve(req.Model)
		if err != nil {
			if errors.Is(err, router.ErrModelNotFound) {
				writeError(w, http.StatusNotFound, fmt.Sprintf("unknown model %q", req.Model), "model_not_found")
				return
			}
			s.logger.Error("router resolve failed", "error", err, "model", req.Model)
			writeError(w, http.StatusInternalServerError, "internal server error", "internal_error")
			return
		}

		providerReq := api.ToProviderRequest(req)

		if !req.Stream {
			resp, err := p.ChatCompletion(r.Context(), providerReq)
			if err != nil {
				s.writeProviderError(w, err)
				return
			}
			writeJSON(w, http.StatusOK, api.FromProviderResponse(resp))
			return
		}

		s.streamChatCompletions(w, r, p, providerReq)
	}
}

// streamChatCompletions serves the SSE path. It validates that w
// supports flushing and confirms the upstream stream opens
// successfully before writing anything, so either failure (writer
// doesn't support streaming, or the provider rejects the request,
// e.g. rate limiting) can still be reported through the normal JSON
// error path — no bytes of a 200 response have been committed yet.
// Only once both checks pass does it flush the SSE headers, so the
// client's connection is established immediately even if the first
// chunk takes a while to arrive, then forwards each upstream chunk as
// an OpenAI-compatible "data: {...}\n\n" frame, flushing after every
// write so the client receives content incrementally instead of
// buffered until the handler returns. While waiting on a slow
// upstream, it emits a ": keep-alive" comment every heartbeat interval
// so the connection isn't dropped as idle by a proxy, load balancer,
// or the client itself.
func (s *Server) streamChatCompletions(w http.ResponseWriter, r *http.Request, p provider.Provider, req *provider.ChatRequest) {
	sw, err := newSSEWriter(w)
	if err != nil {
		s.logger.Error("streaming not supported by response writer", "error", err)
		writeError(w, http.StatusInternalServerError, "streaming not supported", "internal_error")
		return
	}

	stream, err := p.ChatCompletionStream(r.Context(), req)
	if err != nil {
		s.writeProviderError(w, err)
		return
	}
	defer stream.Close()

	sw.WriteHeader()

	reqID := middleware.GetReqID(r.Context())
	id := "chatcmpl-" + reqID

	heartbeat := s.sseHeartbeatInterval
	if heartbeat <= 0 {
		heartbeat = defaultSSEHeartbeatInterval
	}
	ticker := time.NewTicker(heartbeat)
	defer ticker.Stop()

	next := recvAsync(stream)
	for {
		select {
		case res := <-next:
			if res.err == io.EOF {
				if err := sw.WriteDone(); err != nil {
					s.logger.Error("stream write failed", "error", err, "request_id", reqID)
				}
				return
			}
			if res.err != nil {
				// Headers and a 200 status are already committed at this
				// point, so an upstream failure mid-stream can only be
				// surfaced by logging and closing the connection early —
				// there is no HTTP-level error status left to send.
				s.logger.Error("stream read failed", "error", res.err, "request_id", reqID)
				return
			}

			chunk := api.FromProviderStreamChunk(id, req.Model, res.chunk)
			if err := sw.WriteEvent(chunk); err != nil {
				s.logger.Error("stream write failed", "error", err, "request_id", reqID)
				return
			}
			// A real chunk just arrived, so the connection is
			// demonstrably alive; push the next heartbeat back out
			// rather than potentially firing one right after this write.
			ticker.Reset(heartbeat)
			next = recvAsync(stream)

		case <-ticker.C:
			if err := sw.WriteComment("keep-alive"); err != nil {
				s.logger.Error("stream heartbeat write failed", "error", err, "request_id", reqID)
				return
			}
		}
	}
}

// streamRecvResult is the result of one asynchronous
// provider.StreamReader.Recv call.
type streamRecvResult struct {
	chunk *provider.StreamChunk
	err   error
}

// recvAsync calls stream.Recv() in its own goroutine and reports the
// result on the returned channel. provider.StreamReader.Recv is a
// blocking call with no channel or context variant, so this is what
// lets streamChatCompletions wait on it inside a select alongside a
// heartbeat ticker. The channel is buffered so the goroutine can
// always send its result and exit even if the caller stops reading
// (e.g. after a write error ends the stream early).
func recvAsync(stream provider.StreamReader) <-chan streamRecvResult {
	ch := make(chan streamRecvResult, 1)
	go func() {
		chunk, err := stream.Recv()
		ch <- streamRecvResult{chunk: chunk, err: err}
	}()
	return ch
}

// writeProviderError maps an error returned by a provider.Provider to
// this package's standard error JSON shape and an appropriate HTTP
// status, using the same status family the provider originally saw
// (rate limit, invalid request, or unavailable/unknown).
func (s *Server) writeProviderError(w http.ResponseWriter, err error) {
	var perr *provider.ProviderError
	if errors.As(err, &perr) {
		status := http.StatusBadGateway
		errType := "provider_unavailable"
		switch {
		case errors.Is(perr, provider.ErrRateLimited):
			status = http.StatusTooManyRequests
			errType = "rate_limited"
		case errors.Is(perr, provider.ErrInvalidRequest):
			status = http.StatusBadRequest
			errType = "invalid_request"
		}
		writeError(w, status, perr.Error(), errType)
		return
	}

	s.logger.Error("unexpected provider error", "error", err)
	writeError(w, http.StatusInternalServerError, "internal server error", "internal_error")
}
