package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"video-pipeline/internal/config"
	"video-pipeline/internal/events"
	"video-pipeline/internal/httpserver"
	"video-pipeline/internal/job"
	"video-pipeline/internal/queue"
	"video-pipeline/internal/retention"
	"video-pipeline/internal/storage"
	"video-pipeline/internal/transcoder"
	"video-pipeline/internal/worker"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st, err := newStorage(ctx, cfg)
	if err != nil {
		slog.Error("failed to init storage", "error", err)
		os.Exit(1)
	}

	jobstore := job.NewMemoryStore()
	q := queue.NewChannel(100)
	tc := transcoder.New()
	broker := events.NewBroker()
	pool := worker.NewPool(q, jobstore, st, tc, broker, 3, 15*time.Minute)
	var wg sync.WaitGroup

	wg.Add(3)
	go func() {
		defer wg.Done()
		pool.Start(ctx)
	}()

	go func() {
		defer wg.Done()
		retention.Run(ctx, jobstore, st, 1*time.Hour, 48*time.Hour)
	}()

	go func() {
		defer wg.Done()
		retention.RunStaleUploads(ctx, jobstore, st, 10*time.Minute, 1*time.Hour)
	}()

	srv := httpserver.New(cfg, st, jobstore, q, tc, broker)
	if err := srv.Run(ctx); err != nil {
		slog.Error("server exited with error", "error", err)
		os.Exit(1)
	}
	wg.Wait()

	slog.Info("shutdown complete")
}

// newStorage picks the Storage implementation based on config, so the
// same binary runs against local disk in dev and B2 in production with
// no code changes — just an env var flip.
func newStorage(ctx context.Context, cfg *config.Config) (storage.Storage, error) {
	switch cfg.StorageBackend {
	case "b2":
		return storage.NewB2(ctx, storage.B2Config{
			Endpoint:       cfg.B2Endpoint,
			Region:         cfg.B2Region,
			Bucket:         cfg.B2Bucket,
			KeyID:          cfg.B2KeyID,
			ApplicationKey: cfg.B2AppKey,
		})
	case "local":
		return storage.NewLocal("./data")
	default:
		return nil, fmt.Errorf("unknown STORAGE_BACKEND %q (want \"local\" or \"b2\")", cfg.StorageBackend)
	}
}
