package server

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/yourname/rate-limiter-gateway/config"
)

// Server wraps a standard http.Server and adds graceful shutdown support.
type Server struct {
	httpServer      *http.Server
	shutdownTimeout time.Duration
}

// New creates a Server from the provided config and HTTP handler.
func New(cfg config.ServerConfig, handler http.Handler) *Server {
	shutdownTimeout := cfg.ShutdownTimeout
	if shutdownTimeout == 0 {
		shutdownTimeout = 30 * time.Second
	}

	return &Server{
		httpServer: &http.Server{
			Addr:         fmt.Sprintf(":%d", cfg.Port),
			Handler:      handler,
			ReadTimeout:  cfg.ReadTimeout,
			WriteTimeout: cfg.WriteTimeout,
			IdleTimeout:  cfg.IdleTimeout,
		},
		shutdownTimeout: shutdownTimeout,
	}
}

// Start begins listening and serving, then blocks until the process receives
// SIGINT or SIGTERM, at which point it performs a graceful shutdown.
func (s *Server) Start() error {
	// Channel that closes when the server goroutine exits.
	serverErr := make(chan error, 1)

	go func() {
		log.Printf("[server] listening on %s", s.httpServer.Addr)
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			serverErr <- err
		}
		close(serverErr)
	}()

	// Wait for OS signal or a fatal server error.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-serverErr:
		return fmt.Errorf("server error: %w", err)
	case sig := <-quit:
		log.Printf("[server] received signal %s – shutting down", sig)
	}

	return s.Shutdown(context.Background())
}

// Shutdown gracefully drains active connections within the configured timeout.
func (s *Server) Shutdown(parent context.Context) error {
	ctx, cancel := context.WithTimeout(parent, s.shutdownTimeout)
	defer cancel()

	if err := s.httpServer.Shutdown(ctx); err != nil {
		return fmt.Errorf("graceful shutdown failed: %w", err)
	}
	log.Printf("[server] shutdown complete")
	return nil
}

// Addr returns the address the server is configured to listen on.
func (s *Server) Addr() string {
	return s.httpServer.Addr
}
