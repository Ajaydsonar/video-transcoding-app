// Package retention deletes old jobs and their associated files so an
// unauthenticated public demo doesn't accumulate storage forever.
package retention

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"video-pipeline/internal/job"
	"video-pipeline/internal/storage"
)

// Run blocks, deleting jobs (and their files) older than ttl every
// interval, until ctx is cancelled. Same shutdown shape as everything
// else in this codebase — call it in its own goroutine from main, tracked
// by the same sync.WaitGroup pattern you already use for the worker pool.
func Run(ctx context.Context, js job.Store, st storage.Storage, interval, ttl time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			sweep(ctx, js, st, ttl)
		case <-ctx.Done():
			return
		}
	}
}

func sweep(ctx context.Context, js job.Store, st storage.Storage, ttl time.Duration) {
	jobs, err := js.ListOlderThan(ctx, time.Now().UTC().Add(-ttl))
	if err != nil {
		slog.Error("retention: listing expired jobs failed", "error", err)
		return
	}

	for _, j := range jobs {
		// Delete files FIRST, job record last. If a file delete fails
		// partway through, the job record staying around means the next
		// sweep will find it again and retry — deleting the record first
		// would orphan any files that failed to clean up, with nothing
		// left that remembers they exist.
		if err := deleteJobFiles(ctx, st, j); err != nil {
			slog.Error("retention: deleting job files failed, will retry next sweep", "job_id", j.ID, "error", err)
			continue
		}

		if err := js.Delete(ctx, j.ID); err != nil {
			slog.Error("retention: deleting job record failed", "job_id", j.ID, "error", err)
			continue
		}
		slog.Info("retention: deleted expired job", "job_id", j.ID, "age", time.Since(j.CreatedAt))
	}
}

func deleteJobFiles(ctx context.Context, st storage.Storage, j job.Job) error {
	if j.RawKey != "" {
		if err := st.Delete(ctx, j.RawKey); err != nil {
			return fmt.Errorf("deleting raw file: %w", err)
		}
	}
	for _, out := range j.Outputs {
		key := fmt.Sprintf("processed/%s/%s.mp4", j.ID, out.Name)
		if err := st.Delete(ctx, key); err != nil {
			return fmt.Errorf("deleting rendition %s: %w", out.Name, err)
		}
	}
	return nil
}
