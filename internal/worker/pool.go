// Package worker
package worker

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"video-pipeline/internal/events"
	"video-pipeline/internal/job"
	"video-pipeline/internal/queue"
	"video-pipeline/internal/storage"
	"video-pipeline/internal/transcoder"
)

type Pool struct {
	q          queue.Queue
	js         job.Store
	st         storage.Storage
	tc         *transcoder.FFmpeg
	b          *events.Broker
	numWorkers int
	jobTimeout time.Duration
}

func NewPool(q queue.Queue, js job.Store, st storage.Storage, tc *transcoder.FFmpeg, b *events.Broker, numWorkers int, jobTimeout time.Duration) *Pool {
	return &Pool{
		q:          q,
		js:         js,
		st:         st,
		tc:         tc,
		b:          b,
		numWorkers: numWorkers,
		jobTimeout: jobTimeout,
	}
}

func (p *Pool) Start(ctx context.Context) {
	var wg sync.WaitGroup
	for i := 0; i < p.numWorkers; i++ {
		wg.Add(1)
		go runWorker(ctx, &wg, p.q, p.js, p.st, p.tc, p.b, p.jobTimeout)
	}
	wg.Wait()
}

func runWorker(ctx context.Context, wg *sync.WaitGroup, q queue.Queue, js job.Store, st storage.Storage, tc *transcoder.FFmpeg, b *events.Broker, jobTimeout time.Duration) {
	defer wg.Done()
	for {
		id, err := q.Dequeue(ctx)
		if err != nil {
			return // ctx cancelled — shutting down
		}
		processJob(ctx, id, js, st, tc, b, jobTimeout)
	}
}

// processJob drives one job through processing -> completed (or failed).
// It never returns an error itself — a failed job is a valid outcome,
// recorded on the Job, not a reason to crash the worker goroutine. If
// this worker died on every bad video, one malformed upload would
// permanently shrink your pool by one.
func processJob(
	ctx context.Context,
	id string,
	js job.Store,
	st storage.Storage,
	tc *transcoder.FFmpeg,
	b *events.Broker,
	jobTimeout time.Duration,
) {
	j, err := js.Get(ctx, id)
	if err != nil {
		slog.Error(
			"worker: could not load job",
			"job_id", id,
			"error", err,
		)
		return
	}

	j.Status = job.StatusProcessing
	j.Progress = 0
	j.UpdatedAt = time.Now().UTC()

	if err := js.Update(ctx, &j); err != nil {
		slog.Error(
			"worker: failed to mark job processing",
			"job_id", id,
			"error", err,
		)
		return
	}

	b.Publish(events.Update{
		JobID:    j.ID,
		Status:   string(job.StatusProcessing),
		Progress: 0,
	})

	// Each job gets its own timeout.
	jobCtx, cancel := context.WithTimeout(ctx, jobTimeout)
	defer cancel()

	var transcodeErr error

	switch j.Kind {
	case job.KindMP4:
		transcodeErr = runTranscode(
			jobCtx,
			&j,
			js,
			st,
			tc,
			b,
		)

	case job.KindHLS:
		transcodeErr = runTranscodeHLS(
			jobCtx,
			&j,
			js,
			st,
			tc,
			b,
		)

	default:
		transcodeErr = fmt.Errorf(
			"unsupported job kind %q",
			j.Kind,
		)
	}

	// One and ONLY one place handles failure.
	if transcodeErr != nil {
		slog.Error(
			"worker: transcode failed",
			"job_id", id,
			"kind", j.Kind,
			"error", transcodeErr,
		)

		j.Status = job.StatusFailed
		j.Error = transcodeErr.Error()
		j.UpdatedAt = time.Now().UTC()

		if err := js.Update(ctx, &j); err != nil {
			slog.Error(
				"worker: failed to mark job failed",
				"job_id", id,
				"error", err,
			)
			return
		}

		b.Publish(events.Update{
			JobID:    j.ID,
			Status:   string(job.StatusFailed),
			Progress: j.Progress,
		})

		return
	}

	// Both MP4 and HLS functions return successfully only when
	// their complete output has been uploaded.
	j.Status = job.StatusCompleted
	j.Progress = 100
	j.UpdatedAt = time.Now().UTC()

	if err := js.Update(ctx, &j); err != nil {
		slog.Error(
			"worker: failed to mark job completed",
			"job_id", id,
			"error", err,
		)
		return
	}

	b.Publish(events.Update{
		JobID:    j.ID,
		Status:   string(job.StatusCompleted),
		Progress: 100,
	})

	slog.Info(
		"worker: job completed",
		"job_id", id,
		"kind", j.Kind,
	)
}

func runTranscodeHLS(
	ctx context.Context,
	j *job.Job,
	js job.Store,
	st storage.Storage,
	tc *transcoder.FFmpeg,
	b *events.Broker,
) error {
	tmpDir, err := os.MkdirTemp("", "hls-job-*")
	if err != nil {
		return fmt.Errorf("creating temp dir: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	// ------------------------------------------------------------
	// 1. Download raw input
	// ------------------------------------------------------------

	raw, err := st.Get(ctx, j.RawKey)
	if err != nil {
		return fmt.Errorf("fetching raw upload: %w", err)
	}

	inputPath := filepath.Join(tmpDir, "input")

	inFile, err := os.Create(inputPath)
	if err != nil {
		raw.Close()
		return fmt.Errorf("creating local input file: %w", err)
	}

	_, copyErr := io.Copy(inFile, raw)

	raw.Close()

	if err := inFile.Close(); err != nil && copyErr == nil {
		copyErr = err
	}

	if copyErr != nil {
		return fmt.Errorf("copying raw upload locally: %w", copyErr)
	}

	// ------------------------------------------------------------
	// 2. Create HLS output directory
	// ------------------------------------------------------------

	outDir := filepath.Join(tmpDir, "hls")

	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return fmt.Errorf("creating HLS output dir: %w", err)
	}

	// ------------------------------------------------------------
	// 3. Progress callback
	// ------------------------------------------------------------

	// Last band flushed to the store, starting at band 0 (the initial
	// "processing, 0%" mark already persisted it). Bands, not ticks —
	// see inside the closure.
	lastPersisted := 0

	onProgress := func(percent int) {
		if percent < 0 {
			percent = 0
		}
		if percent > 100 {
			percent = 100
		}
		j.Progress = percent

		// Durable milestones, volatile ticks: every percentage still
		// streams to live listeners via the broker, but the store only
		// sees 10% bands (and 100). With a disk-backed store, persisting
		// every single tick would turn one transcode into hundreds of
		// pointless writes; a crash loses at most one band of progress.
		if percent == 100 || percent/10 != lastPersisted/10 {
			lastPersisted = percent
			j.UpdatedAt = time.Now().UTC()

			if err := js.Update(ctx, j); err != nil {
				slog.Warn(
					"worker: HLS progress update failed",
					"job_id", j.ID,
					"error", err,
				)
			}
		}

		b.Publish(events.Update{
			JobID:    j.ID,
			Status:   string(job.StatusProcessing),
			Progress: percent,
		})
	}

	// ------------------------------------------------------------
	// 4. Run FFmpeg
	// ------------------------------------------------------------

	results, err := tc.TranscodeHLS(
		ctx,
		inputPath,
		outDir,
		transcoder.DefaultLadder,
		onProgress,
	)
	if err != nil {
		return fmt.Errorf("HLS transcode: %w", err)
	}

	// ------------------------------------------------------------
	// 5. Upload HLS directory
	// ------------------------------------------------------------

	prefix := fmt.Sprintf(
		"processed/%s/hls",
		j.ID,
	)

	if err := uploadHLSDirectory(
		ctx,
		st,
		outDir,
		prefix,
	); err != nil {
		return fmt.Errorf("uploading HLS output: %w", err)
	}

	// ------------------------------------------------------------
	// 6. Save master playlist key
	// ------------------------------------------------------------

	j.MasterKey = fmt.Sprintf(
		"processed/%s/hls/master.m3u8",
		j.ID,
	)

	// ------------------------------------------------------------
	// 7. Save HLS rendition metadata
	// ------------------------------------------------------------

	j.Outputs = make([]job.Output, 0, len(results))

	for _, r := range results {
		j.Outputs = append(j.Outputs, job.Output{
			Name:      r.Name,
			Width:     r.Width,
			Height:    r.Height,
			Bandwidth: r.Bandwidth,
			SizeBytes: r.Bytes,
		})
	}

	return nil
}

func uploadHLSDirectory(
	ctx context.Context,
	st storage.Storage,
	localDir string,
	storagePrefix string,
) error {
	return filepath.Walk(
		localDir,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if info.IsDir() {
				return nil
			}

			rel, err := filepath.Rel(localDir, path)
			if err != nil {
				return fmt.Errorf(
					"calculating relative path: %w",
					err,
				)
			}

			key := filepath.ToSlash(
				filepath.Join(storagePrefix, rel),
			)

			f, err := os.Open(path)
			if err != nil {
				return fmt.Errorf(
					"opening HLS file %s: %w",
					path,
					err,
				)
			}

			err = st.Put(ctx, key, f)
			closeErr := f.Close()

			if err != nil {
				return fmt.Errorf(
					"uploading HLS file %s: %w",
					key,
					err,
				)
			}

			if closeErr != nil {
				return fmt.Errorf(
					"closing HLS file %s: %w",
					path,
					closeErr,
				)
			}

			return nil
		},
	)
}

// runTranscode does the actual work: pull the raw upload down to a local
// temp file (ffmpeg needs a real file path), run the transcode, then push
// each rendition back up through Storage. Everything under tmpDir is
// cleaned up on the way out, success or failure, via defer.
func runTranscode(ctx context.Context, j *job.Job, js job.Store, st storage.Storage, tc *transcoder.FFmpeg, b *events.Broker) error {
	tmpDir, err := os.MkdirTemp("", "job-*")
	if err != nil {
		return fmt.Errorf("creating temp dir: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	raw, err := st.Get(ctx, j.RawKey)
	if err != nil {
		return fmt.Errorf("fetching raw upload: %w", err)
	}
	inputPath := filepath.Join(tmpDir, "input")
	inFile, err := os.Create(inputPath)
	if err != nil {
		raw.Close()
		return fmt.Errorf("creating local input file: %w", err)
	}
	_, copyErr := io.Copy(inFile, raw)
	raw.Close()
	if err := inFile.Close(); err != nil && copyErr == nil {
		copyErr = err
	}
	if copyErr != nil {
		return fmt.Errorf("copying raw upload locally: %w", copyErr)
	}

	outDir := filepath.Join(tmpDir, "out")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return fmt.Errorf("creating output dir: %w", err)
	}

	// This closure turns ffmpeg's raw percentage stream into visible
	// progress: every tick still streams to live listeners via the
	// broker, but the store only sees 10% bands (and 100) — same
	// milestone rule as the HLS path, so a disk-backed store isn't
	// hammered with a write per tick. Starts at band 0, already
	// persisted by the initial "processing" mark.
	lastPersisted := 0

	onProgress := func(percent int) {
		if percent < 0 {
			percent = 0
		}
		if percent > 100 {
			percent = 100
		}
		j.Progress = percent
		if percent == 100 || percent/10 != lastPersisted/10 {
			lastPersisted = percent
			j.UpdatedAt = time.Now().UTC()
			if err := js.Update(ctx, j); err != nil {
				slog.Warn("worker: progress update failed", "job_id", j.ID, "error", err)
			}
		}

		b.Publish(events.Update{JobID: j.ID, Progress: percent, Status: string(job.StatusProcessing)})
	}

	results, err := tc.Transcode(ctx, inputPath, outDir, transcoder.DefaultLadder, onProgress)
	if err != nil {
		return err
	}

	outputs := make([]job.Output, 0, len(results))
	for _, r := range results {
		filename := r.Name + ".mp4"
		size, err := uploadOutput(ctx, st, j.ID, outDir, filename)
		if err != nil {
			return err
		}
		outputs = append(outputs, job.Output{
			Name:      r.Name,
			Width:     r.Width,
			Height:    r.Height,
			SizeBytes: size,
		})
	}
	j.Outputs = outputs // processJob's existing final js.Update(ctx, &j) call already picks this up
	return nil
}

func uploadOutput(ctx context.Context, st storage.Storage, jobID, outDir, filename string) (int64, error) {
	path := filepath.Join(outDir, filename)

	info, err := os.Stat(path)
	if err != nil {
		return 0, fmt.Errorf("statting output %s: %w", filename, err)
	}

	f, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("opening output %s: %w", filename, err)
	}
	defer f.Close()

	key := fmt.Sprintf("processed/%s/%s", jobID, filename)
	if err := st.Put(ctx, key, f); err != nil {
		return 0, fmt.Errorf("uploading output %s: %w", filename, err)
	}
	return info.Size(), nil
}
