package httpserver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"video-pipeline/internal/events"

	"video-pipeline/internal/config"
	"video-pipeline/internal/job"
	"video-pipeline/internal/queue"
	"video-pipeline/internal/storage"
	"video-pipeline/internal/transcoder"
)

// Server wraps the standard library's http.Server. We keep our own mux on
// the struct (rather than using http.DefaultServeMux) so routes are
// explicit and testable, and so nothing else in the process can quietly
// register a handler we don't know about.
//
// storage and jobs are declared as INTERFACES, not concrete types (*Local,
// *MemoryStore). Handlers never know whether they're talking to disk or
// R2, memory or Postgres — that's the whole point of the abstraction.
type Server struct {
	cfg     *config.Config
	mux     *http.ServeMux
	handler http.Handler
	storage storage.Storage
	jobs    job.Store
	q       queue.Queue
	tc      *transcoder.FFmpeg
	b       *events.Broker
}

func New(cfg *config.Config, st storage.Storage, jobs job.Store, q queue.Queue, tc *transcoder.FFmpeg, b *events.Broker) *Server {
	s := &Server{
		cfg:     cfg,
		mux:     http.NewServeMux(),
		storage: st,
		jobs:    jobs,
		q:       q,
		tc:      tc,
		b:       b,
	}
	s.routes()
	s.handler = withCORS(s.mux, cfg.FrontendOrigins)
	return s
}

// Run starts the HTTP server and BLOCKS until ctx is cancelled (e.g. by a
// SIGINT/SIGTERM from main.go), then performs a graceful shutdown: stop
// accepting new connections, let in-flight requests finish (up to a
// timeout), then return.
func (s *Server) Run(ctx context.Context) error {
	httpSrv := &http.Server{
		Addr:         ":" + s.cfg.Port,
		Handler:      s.handler,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// ListenAndServe blocks, so it needs to run in its own goroutine.
	// We report its result back over a channel rather than a shared
	// variable — channels are how goroutines hand data back safely
	// without needing a mutex.
	errCh := make(chan error, 1)
	go func() {
		slog.Info("http server listening", "addr", httpSrv.Addr, "env", s.cfg.Env)
		err := httpSrv.ListenAndServe()
		// ErrServerClosed is the *expected* error when we call Shutdown()
		// below — it's not a real failure, so we don't treat it as one.
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	// select blocks until ONE of these two things happens first:
	// the server dies on its own (errCh), or someone cancels ctx.
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		slog.Info("shutdown signal received, draining connections")
	}

	// Give in-flight requests up to 10s to finish before we cut the cord.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("graceful shutdown failed: %w", err)
	}
	return nil
}
