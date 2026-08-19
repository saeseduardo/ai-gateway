package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

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

// streamChatCompletions serves the SSE path. It opens the upstream
// stream first, so a failure (e.g. rate limiting) can still be
// reported through the normal JSON error path before any bytes of a
// 200 response are committed. Only once that succeeds does it switch
// the response into text/event-stream and forward each chunk as an
// OpenAI-compatible "data: {...}\n\n" line, flushing after every
// write so the client receives content incrementally instead of
// buffered until the handler returns.
func (s *Server) streamChatCompletions(w http.ResponseWriter, r *http.Request, p provider.Provider, req *provider.ChatRequest) {
	stream, err := p.ChatCompletionStream(r.Context(), req)
	if err != nil {
		s.writeProviderError(w, err)
		return
	}
	defer stream.Close()

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming not supported", "internal_error")
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	reqID := middleware.GetReqID(r.Context())
	id := "chatcmpl-" + reqID

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			fmt.Fprint(w, "data: [DONE]\n\n")
			flusher.Flush()
			return
		}
		if err != nil {
			// Headers and a 200 status are already committed at this
			// point, so an upstream failure mid-stream can only be
			// surfaced by logging and closing the connection early —
			// there is no HTTP-level error status left to send.
			s.logger.Error("stream read failed", "error", err, "request_id", reqID)
			return
		}

		data, err := json.Marshal(api.FromProviderStreamChunk(id, req.Model, chunk))
		if err != nil {
			s.logger.Error("stream marshal failed", "error", err, "request_id", reqID)
			return
		}
		fmt.Fprintf(w, "data: %s\n\n", data)
		flusher.Flush()
	}
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
