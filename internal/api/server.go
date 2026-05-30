package api

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
)

// Server wraps http.Server with Start/Shutdown helpers.
type Server struct {
	srv *http.Server
}

// NewServer creates a Server that will listen on addr.
func NewServer(addr string, handler http.Handler) *Server {
	return &Server{
		srv: &http.Server{
			Addr:    addr,
			Handler: handler,
		},
	}
}

// Start binds the listener synchronously (so the port is ready on return),
// then serves in a background goroutine.  Any post-bind serve errors are
// logged but not propagated — the caller shuts down via Shutdown.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.srv.Addr)
	if err != nil {
		return err
	}
	go func() {
		if err := s.srv.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
			slog.Error("http serve error", "err", err)
		}
	}()
	return nil
}

// Shutdown gracefully drains active connections.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}
