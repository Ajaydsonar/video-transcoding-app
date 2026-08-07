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
	broker     *events.Broker
	numWorkers int
}

func NewPool(q queue.Queue, js job.Store, st storage.Storage, tc *transcoder.FFmpeg, broker *events.Broker, numWorkers int) *Pool {
	return &Pool{
		q:          q,
		js:         js,
		st:         st,
		tc:         tc,
		broker:     broker,
		numWorkers: numWorkers,
	}
}

func (p *Pool) Start(ctx context.Context) {
	var wg sync.WaitGroup

	for i := 0; i < p.numWorkers; i++ {
		wg.Add(1)
		go runWorker(ctx, &wg, p.q, p.js, p.st, p.tc, p.broker)
	}

	wg.Wait()
}

func runWorker(ctx context.Context, wg *sync.WaitGroup, q queue.Queue, js job.Store, st storage.Storage, tc *transcoder.FFmpeg, b *events.Broker) {
	defer wg.Done()

	for {
		id, err := q.Dequeue(ctx)
		if err != nil {
			return
		}

		processJob(ctx, id, js, st, tc, b)
	}
}

func processJob(ctx context.Context, id string, js job.Store, st storage.Storage, tc *transcoder.FFmpeg, b *events.Broker) {
	j, err := js.Get(ctx, id)
	if err != nil {
		slog.Error("worker: could not load job", "job_id", id, "error", err)
		return
	}

	j.Status = job.StatusProcessing
	j.UpdatedAt = time.Now().UTC()

	if err := js.Update(ctx, &j); err != nil {
		slog.Error("worker: failed to mark job processing", "job_id", id, "error", err)
		return
	}

	b.Publish(events.Update{JobID: j.ID, Status: string(job.StatusProcessing), Progress: 0})

	if err := runTranscode(ctx, &j, js, st, tc, b); err != nil {
		slog.Error("worker: transcode failed", "job_id", id, "error", err)

		b.Publish(events.Update{JobID: j.ID, Status: string(job.StatusFailed)})

		j.Status = job.StatusFailed
		j.Error = err.Error()
		j.UpdatedAt = time.Now().UTC()
		if err := js.Update(ctx, &j); err != nil {
			slog.Error("worker: failed to mark job failed", "job_id", id, "error", err)
			return
		}
		return
	}

	b.Publish(events.Update{JobID: j.ID, Status: string(job.StatusCompleted), Progress: 100})
	// Mark job as completed
	// j.Progress = 100
	j.Status = job.StatusCompleted
	j.UpdatedAt = time.Now().UTC()

	if err := js.Update(ctx, &j); err != nil {
		slog.Error("worker: failed to mark job completed", "job_id", id, "error", err)
		return
	}
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
	inFile.Close()
	if copyErr != nil {
		return fmt.Errorf("copying raw upload locally: %w", copyErr)
	}

	outDir := filepath.Join(tmpDir, "out")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return fmt.Errorf("creating output dir: %w", err)
	}

	// This closure is what turns ffmpeg's raw percentage stream into a
	// visible, pollable job status: every tick, persist it.
	onProgress := func(percent int) {
		b.Publish(events.Update{JobID: j.ID, Progress: percent})

		j.Progress = percent
		j.UpdatedAt = time.Now().UTC()

		if err := js.Update(ctx, j); err != nil {
			slog.Warn("worker: progress update failed", "job_id", j.ID, "error", err)
		}
	}

	result, err := tc.Transcode(ctx, inputPath, outDir, transcoder.DefaultLadder, onProgress)
	if err != nil {
		return err
	}
	//
	// entries, err := os.ReadDir(outDir)
	// if err != nil {
	// 	return fmt.Errorf("reading transcoded output: %w", err)
	// }
	//
	outputs := make([]job.Output, 0, len(result))
	for _, r := range result {
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

	j.Outputs = outputs

	return nil
}

func uploadOutput(ctx context.Context, st storage.Storage, jobID, outDir, filename string) (int64, error) {
	filepath := filepath.Join(outDir, filename)

	info, err := os.Stat(filepath)
	if err != nil {
		return 0, fmt.Errorf("statting output %s: %w", filename, err)
	}

	f, err := os.Open(filepath)
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
