package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"video-pipeline/internal/config"
	"video-pipeline/internal/events"
	"video-pipeline/internal/httpserver"
	"video-pipeline/internal/job"
	"video-pipeline/internal/logger"
	"video-pipeline/internal/queue"
	"video-pipeline/internal/storage"
	"video-pipeline/internal/transcoder"
	"video-pipeline/internal/worker"
)

func main() {
	// slog is the standard library's structured logger (since Go 1.21).
	// JSON output means logs are grep/parse-friendly in production from
	// day one, instead of retrofitting structure later.
	bootlogger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(bootlogger)

	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	logger.Init(*cfg)
	slog.Info("Application starting up...", "env", cfg.Env)

	// signal.NotifyContext gives us a context that is automatically
	// cancelled the moment the process receives SIGINT (Ctrl+C) or
	// SIGTERM (what Docker/Kubernetes send on a graceful stop).
	// Everything downstream just watches ctx.Done() — no manual
	// signal-channel plumbing needed.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Concrete implementations get built ONCE, here, in main — the only
	// place in the whole app that knows we're using local disk + memory
	// instead of R2 + Postgres. Swapping later means changing these two
	// lines, nothing else.

	st, err := newStorage(ctx, cfg)
	if err != nil {
		slog.Error("failed to init storage", "error", err)
		os.Exit(1)
	}

	jobstore := job.NewMemoryStore()

	q := queue.NewChannel(100)

	tc := transcoder.New()

	broker := events.NewBroker()

	pool := worker.NewPool(q, jobstore, st, tc, broker, 3)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		pool.Start(ctx)
	}()

	srv := httpserver.New(cfg, st, jobstore, q, broker)

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
		slog.Info("Using B2 Storage")

		return storage.NewB2(ctx, storage.B2Config{
			Endpoint:       cfg.B2Endpoint,
			Region:         cfg.B2Region,
			Bucket:         cfg.B2Bucket,
			KeyID:          cfg.B2KeyID,
			ApplicationKey: cfg.B2AppKey,
		})
	case "local":
		slog.Info("Using local Storage")

		return storage.NewLocal("./data/videos")
	default:
		return nil, fmt.Errorf("unknown STORAGE_BACKEND %q (want \"local\" or \"b2\")", cfg.StorageBackend)
	}
}
