package httpserver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"video-pipeline/internal/job"
	"video-pipeline/internal/storage"
)

// RecoverInterruptedJobs re-drives jobs left non-terminal by a crash or
// restart. It runs once at startup, before serving traffic:
//
//   - pending: was queued in the (memory-only) channel, which died with
//     the process — just re-enqueue.
//   - processing: ffmpeg died mid-transcode; the raw object is intact in
//     storage, so reset to pending and re-enqueue (partial outputs are
//     overwritten under the same keys).
//   - validating: the background probe died; the raw object is intact,
//     so resume validation in the background (missing object → failed).
//   - uploading: left alone — the client may still be PUTting (the
//     presigned URL survives restarts) and the stale-upload reaper
//     collects sessions the client abandons by age.
//   - completed/failed: terminal, never touched.
func (s *Server) RecoverInterruptedJobs(ctx context.Context) error {
	jobs, err := s.jobs.ListRecent(ctx, 0)
	if err != nil {
		return fmt.Errorf("recover: listing jobs: %w", err)
	}

	var requeued, revalidated, failed int
	for _, j := range jobs {
		switch j.Status {
		case job.StatusPending:
			if err := s.q.Enqueue(ctx, j.ID); err != nil {
				slog.Error("recover: re-enqueue failed", "job_id", j.ID, "error", err)
				continue
			}
			requeued++

		case job.StatusProcessing:
			j.Status = job.StatusPending
			j.UpdatedAt = time.Now().UTC()
			if err := s.jobs.Update(ctx, &j); err != nil {
				slog.Error("recover: reset to pending failed", "job_id", j.ID, "error", err)
				continue
			}
			if err := s.q.Enqueue(ctx, j.ID); err != nil {
				slog.Error("recover: re-enqueue failed", "job_id", j.ID, "error", err)
				continue
			}
			requeued++

		case job.StatusValidating:
			size, err := s.storage.Stat(ctx, j.RawKey)
			if err != nil {
				if !errors.Is(err, storage.ErrNotFound) {
					slog.Error("recover: stat failed", "job_id", j.ID, "error", err)
					continue
				}
				j.Status = job.StatusFailed
				j.Error = "server restarted before the upload arrived; please upload again"
				j.UpdatedAt = time.Now().UTC()
				if uErr := s.jobs.Update(ctx, &j); uErr != nil {
					slog.Error("recover: marking job failed", "job_id", j.ID, "error", uErr)
					continue
				}
				failed++
				continue
			}
			go s.validateAndEnqueue(j.ID, j.RawKey, size)
			revalidated++
		}
	}

	slog.Info("recover: interrupted jobs re-driven",
		"requeued", requeued, "revalidated", revalidated, "failed", failed)
	return nil
}
