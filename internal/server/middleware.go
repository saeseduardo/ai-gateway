package server

import (
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/go-chi/chi/v5/middleware"
)

// recoverer returns middleware that recovers from panics in downstream
// handlers, logs them through slog, and responds with a 500 in this
// package's standard error JSON shape.
//
// We don't use chi's built-in middleware.Recoverer: it writes a
// human-formatted panic trace straight to os.Stdout, bypassing slog
// entirely. That would break structured/JSON logging and mix two log
// formats in the same stream. This version routes panics through the
// same *slog.Logger (and the same JSON format) as every other log line
// the gateway emits.
func recoverer(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				rvr := recover()
				if rvr == nil {
					return
				}
				// http.ErrAbortHandler is a sentinel panic value used to
				// abort a handler without it being treated as an error
				// worth logging or responding to; let it keep unwinding.
				if rvr == http.ErrAbortHandler {
					panic(rvr)
				}

				logger.Error("panic recovered",
					"panic", fmt.Sprintf("%v", rvr),
					"request_id", middleware.GetReqID(r.Context()),
					"path", r.URL.Path,
					"stack", string(debug.Stack()),
				)
				writeError(w, http.StatusInternalServerError, "internal server error", "internal_error")
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// requestLogger returns middleware that logs one structured line per
// completed request: method, path, status, bytes written, duration,
// and request ID. It also echoes the request ID back as a response
// header: chi's middleware.RequestID only stores the ID in context for
// server-side use, it does not expose it to the client on its own.
//
// It wraps the ResponseWriter with chi's middleware.NewWrapResponseWriter
// because the plain http.ResponseWriter interface only allows writing a
// status/body, not reading them back afterward — wrapping is the only
// way to observe what the handler actually sent.
//
// This middleware is placed after recoverer in the chain (recoverer
// wraps it), so if a handler panics, execution never returns to the
// line below next.ServeHTTP and this access-log line is skipped for
// that request; recoverer's own Error-level log (with path and
// request_id) is what records it instead.
func requestLogger(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			reqID := middleware.GetReqID(r.Context())
			if reqID != "" {
				w.Header().Set(middleware.RequestIDHeader, reqID)
			}
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)

			next.ServeHTTP(ww, r)

			logger.Info("request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", ww.Status(),
				"bytes", ww.BytesWritten(),
				"duration_ms", time.Since(start).Milliseconds(),
				"request_id", reqID,
			)
		})
	}
}
