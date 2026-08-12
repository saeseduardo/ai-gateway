// Package server provides the gateway's HTTP server infrastructure:
// wiring, middleware, lifecycle management, and liveness/readiness
// endpoints. It defines no chat/completion routes and no provider
// integrations — callers mount those on the router returned by
// Server.Router in later modules.
package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/saeseduardo/ai-gateway/internal/config"
)

// Server wraps an *http.Server with the gateway's routing, middleware,
// and graceful-shutdown lifecycle.
type Server struct {
	httpServer      *http.Server
	router          chi.Router
	logger          *slog.Logger
	shutdownTimeout time.Duration
}

// New builds a Server from cfg. The returned Server is not yet
// listening; call Run (or Start) to do so.
func New(cfg config.ServerConfig, logger *slog.Logger) *Server {
	router := chi.NewRouter()

	// Order matters: RequestID/RealIP populate context that recoverer
	// and requestLogger both rely on (request_id), so they must run
	// first. recoverer then wraps requestLogger so that a panic
	// anywhere downstream — including, in principle, in requestLogger
	// itself — is always caught.
	router.Use(middleware.RequestID)
	router.Use(middleware.RealIP)
	router.Use(recoverer(logger))
	router.Use(requestLogger(logger))

	router.Get("/healthz", healthzHandler)
	router.Get("/readyz", readyzHandler)

	httpServer := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Port),
		Handler:      router,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		IdleTimeout:  cfg.IdleTimeout,
	}

	return &Server{
		httpServer:      httpServer,
		router:          router,
		logger:          logger,
		shutdownTimeout: cfg.ShutdownTimeout,
	}
}

// Router returns the underlying chi.Router so other modules can mount
// additional routes (e.g. chat completions) after construction.
func (s *Server) Router() chi.Router {
	return s.router
}

// Start blocks serving HTTP until the server is shut down or fails.
//
// http.ErrServerClosed is what ListenAndServe always returns after a
// successful call to Shutdown; it signals a deliberate, clean stop
// rather than a failure, so it is swallowed here instead of being
// returned as an error.
func (s *Server) Start() error {
	if err := s.httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Shutdown gracefully stops the server, waiting for in-flight requests
// to complete or ctx to be done, whichever comes first.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

// Run starts the server and blocks until it exits: either because
// Start itself failed (e.g. the port is already in use), or because a
// SIGINT/SIGTERM was received and the subsequent graceful shutdown
// completed (or its deadline, cfg.ShutdownTimeout, elapsed).
func (s *Server) Run() error {
	errCh := make(chan error, 1)
	go func() {
		s.logger.Info("server starting", "addr", s.httpServer.Addr)
		errCh <- s.Start()
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	select {
	case err := <-errCh:
		return err

	case sig := <-sigCh:
		s.logger.Info("shutdown signal received", "signal", sig.String())

		ctx, cancel := context.WithTimeout(context.Background(), s.shutdownTimeout)
		defer cancel()

		if err := s.Shutdown(ctx); err != nil {
			return err
		}

		// Wait for Start's goroutine to actually return so Run doesn't
		// report completion before ListenAndServe has unwound.
		if err := <-errCh; err != nil {
			return err
		}

		s.logger.Info("server shutdown complete")
		return nil
	}
}
