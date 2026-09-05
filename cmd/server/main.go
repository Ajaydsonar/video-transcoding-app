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

	jobstore, closeStore, err := newJobStore(cfg)
	if err != nil {
		slog.Error("failed to init job store", "error", err)
		os.Exit(1)
	}
	defer closeStore()

	q := queue.NewChannel(100)
	tc := transcoder.New()
	broker := events.NewBroker()
	pool := worker.NewPool(q, jobstore, st, tc, broker, cfg.Workers, time.Duration(cfg.JobTimeoutMin)*time.Minute)
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

	// Re-drive jobs left non-terminal by a previous crash/restart
	// before accepting traffic. Enqueue only buffers into the channel,
	// so this is safe with workers already running.
	if err := srv.RecoverInterruptedJobs(ctx); err != nil {
		slog.Error("job recovery failed", "error", err)
		os.Exit(1)
	}

	if err := srv.Run(ctx); err != nil {
		slog.Error("server exited with error", "error", err)
		os.Exit(1)
	}
	wg.Wait()

	slog.Info("shutdown complete")
}

// newJobStore picks the job record backend. SQLite is the default:
// records survive crashes (and power recovery); "memory" keeps the old
// ephemeral behavior for throwaway local runs.
func newJobStore(cfg *config.Config) (job.Store, func(), error) {
	switch cfg.JobStore {
	case "sqlite":
		s, err := job.OpenSQLite(cfg.DBPath)
		if err != nil {
			return nil, nil, err
		}
		return s, func() {
			if err := s.Close(); err != nil {
				slog.Error("failed to close job store", "error", err)
			}
		}, nil
	case "memory":
		return job.NewMemoryStore(), func() {}, nil
	default:
		return nil, nil, fmt.Errorf("unknown JOB_STORE %q (want \"sqlite\" or \"memory\")", cfg.JobStore)
	}
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
