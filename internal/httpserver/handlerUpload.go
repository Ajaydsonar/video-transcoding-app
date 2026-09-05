package httpserver

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"video-pipeline/internal/events"
	"video-pipeline/internal/job"
	"video-pipeline/internal/storage"
)

const (
	// maxUploadSize caps the client-declared file size for a direct
	// upload. The complete step re-checks the real object size against
	// this — a lying client can't smuggle a bigger file past us.
	maxUploadSize = 2 << 30 // 2GB
	// maxQueueDepth rejects new upload sessions before they're created
	// when workers are already backed up this deep.
	maxQueueDepth = 20
	// presignExpiry bounds how long the client has to finish its direct
	// PUT. The stale-upload reaper (retention package) cleans up
	// sessions older than a multiple of this.
	presignExpiry = 15 * time.Minute
)

// createUploadRequest is the JSON body for POST /videos: what the client
// wants to upload, not the bytes themselves. Those go straight to object
// storage via the presigned URL we hand back.
type createUploadRequest struct {
	Filename string   `json:"filename"`
	Size     int64    `json:"size"`
	Kind     job.Kind `json:"kind"`
}

// uploadSession is the 202 response: everything the client needs to
// push its bytes to storage and then tell us it's done.
type uploadSession struct {
	ID        string     `json:"id"`
	Kind      job.Kind   `json:"kind"`
	Status    job.Status `json:"status"`
	UploadURL string     `json:"uploadUrl"`
	ExpiresIn int64      `json:"expiresIn"` // seconds the upload URL stays valid
	RawKey    string     `json:"rawKey"`
}

// handleCreateUpload starts a direct-upload session. It validates the
// *intent* (kind, name, declared size), reserves the storage key with a
// job record in "uploading" state, and returns a presigned PUT URL — the
// bytes themselves never touch this server.
func (s *Server) handleCreateUpload(w http.ResponseWriter, r *http.Request) {
	if s.q.Len() >= maxQueueDepth {
		w.Header().Set("Retry-After", "30")
		writeError(w, http.StatusServiceUnavailable, fmt.Errorf("server is at capacity, try again shortly"))
		return
	}

	// Tiny body — this is JSON metadata, not the video.
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var req createUploadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("parsing request: %w", err))
		return
	}

	kind := req.Kind
	if kind == "" {
		kind = job.KindMP4
	}
	switch kind {
	case job.KindMP4, job.KindHLS:
	default:
		writeError(w, http.StatusBadRequest,
			fmt.Errorf("invalid kind %q: must be mp4 or hls", kind))
		return
	}

	name := filepath.Base(strings.ReplaceAll(req.Filename, "\\", "/"))
	if name == "." || name == ".." || name == "/" || name == "" || len(name) > 255 {
		writeError(w, http.StatusBadRequest, fmt.Errorf("invalid filename %q", req.Filename))
		return
	}
	if req.Size <= 0 {
		writeError(w, http.StatusBadRequest, fmt.Errorf("size must be positive"))
		return
	}
	if req.Size > maxUploadSize {
		writeError(w, http.StatusRequestEntityTooLarge, fmt.Errorf("upload exceeds %d byte limit", maxUploadSize))
		return
	}

	id, err := newID()
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("generating job id: %w", err))
		return
	}
	rawKey := fmt.Sprintf("raw/%s/%s", id, name)

	now := time.Now().UTC()
	j := &job.Job{
		ID:        id,
		Status:    job.StatusUploading,
		RawKey:    rawKey,
		CreatedAt: now,
		UpdatedAt: now,
		Kind:      kind,
	}
	if err := s.jobs.Create(r.Context(), j); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("creating job record: %w", err))
		return
	}

	uploadURL, err := s.storage.PresignPut(r.Context(), rawKey, presignExpiry)
	if err != nil {
		// Don't orphan the session we just created — without an upload
		// URL it can never complete, so remove it right away instead of
		// waiting for the stale-upload reaper.
		if dErr := s.jobs.Delete(r.Context(), id); dErr != nil {
			slog.Error("failed to roll back upload session", "job_id", id, "error", dErr)
		}
		writeError(w, http.StatusInternalServerError, fmt.Errorf("issuing upload URL: %w", err))
		return
	}

	slog.Info("upload session created", "job_id", id, "filename", name, "size", req.Size, "kind", kind)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(uploadSession{
		ID:        id,
		Kind:      kind,
		Status:    job.StatusUploading,
		UploadURL: uploadURL,
		ExpiresIn: int64(presignExpiry / time.Second),
		RawKey:    rawKey,
	})
}

// handleCompleteUpload finishes a direct-upload session: the client has
// PUT its bytes straight to storage, and now asks us to validate and
// queue the job. Validation is server-side (existence, size, ffprobe),
// so a malicious client can't queue garbage by lying in the create call.
//
// Only the cheap checks (HEAD for existence/size) run inline — the slow
// part (downloading the object for ffprobe) runs in a background
// goroutine and the handler answers 202 immediately. A synchronous
// complete would hold the HTTP response open for the whole download
// (tens of seconds to minutes), and any proxy/gateway in front answers
// that with a 502/504 while the job actually queued fine behind it.
//
// Safe to retry: sessions already past "uploading" return the current
// job unchanged.
func (s *Server) handleCompleteUpload(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	j, err := s.jobs.Get(r.Context(), id)
	if err != nil {
		if errors.Is(err, job.ErrNotFound) {
			writeError(w, http.StatusNotFound, err)
			return
		}
		writeError(w, http.StatusInternalServerError, fmt.Errorf("loading job: %w", err))
		return
	}

	if j.Status != job.StatusUploading {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(j)
		return
	}

	// The bytes must actually be there, and sanely sized. These are
	// HEAD-cheap and stay inline. Deliberately non-destructive: a client
	// that calls complete a hair too early (PUT still in flight) gets a
	// plain 400 and can simply retry — the session stays alive until
	// the presigned URL / stale-upload reaper cleans it up.
	size, err := s.storage.Stat(r.Context(), j.RawKey)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			writeError(w, http.StatusBadRequest, fmt.Errorf("upload not finished: no object at %s yet, retry once the PUT completes", j.RawKey))
			return
		}
		writeError(w, http.StatusInternalServerError, fmt.Errorf("stating upload: %w", err))
		return
	}
	if size <= 0 || size > maxUploadSize {
		writeError(w, http.StatusBadRequest, fmt.Errorf("uploaded object has invalid size %d, re-upload and retry", size))
		return
	}

	// Slow part goes async: mark validating, answer 202, probe + enqueue
	// in the background. The client's poll/SSE flow already handles any
	// non-terminal status, so no frontend change is needed.
	j.Status = job.StatusValidating
	j.UpdatedAt = time.Now().UTC()
	if err := s.jobs.Update(r.Context(), &j); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("marking job validating: %w", err))
		return
	}

	s.b.Publish(events.Update{
		JobID:    j.ID,
		Status:   string(job.StatusValidating),
		Progress: 0,
	})

	go s.validateAndEnqueue(j.ID, j.RawKey, size)

	slog.Info("upload accepted for validation", "job_id", j.ID, "size", size)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(j)
}

// validateAndEnqueue runs the slow half of complete off the request
// path: download the object, ffprobe it, then mark pending + enqueue —
// or fail the job (and delete the object) when the bytes aren't a real
// video. It owns the job from "validating" onward; no other writer
// touches the record in that window, so no locking beyond the store's
// own mutex is needed.
func (s *Server) validateAndEnqueue(id, rawKey string, size int64) {
	// Detached from the request: the client got its 202 long ago and may
	// be gone; this work must not die with its connection.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	fail := func(format string, args ...any) {
		msg := fmt.Sprintf(format, args...)
		if dErr := s.storage.Delete(ctx, rawKey); dErr != nil {
			slog.Error("failed to delete invalid upload", "job_id", id, "error", dErr)
		}
		j, err := s.jobs.Get(ctx, id)
		if err != nil {
			slog.Error("failed to load job for failure marking", "job_id", id, "error", err)
			return
		}
		j.Status = job.StatusFailed
		j.Error = msg
		j.UpdatedAt = time.Now().UTC()
		if uErr := s.jobs.Update(ctx, &j); uErr != nil {
			slog.Error("failed to mark job failed", "job_id", id, "error", uErr)
			return
		}
		s.b.Publish(events.Update{
			JobID:    id,
			Status:   string(job.StatusFailed),
			Progress: j.Progress,
		})
	}

	if err := s.validateVideo(ctx, rawKey); err != nil {
		slog.Warn("upload rejected: not a valid video", "job_id", id, "error", err)
		fail("file doesn't look like a valid video: %v", err)
		return
	}

	j, err := s.jobs.Get(ctx, id)
	if err != nil {
		slog.Error("failed to load validated job", "job_id", id, "error", err)
		return
	}

	j.Status = job.StatusPending
	j.Progress = 0
	j.UpdatedAt = time.Now().UTC()
	if err := s.jobs.Update(ctx, &j); err != nil {
		slog.Error("failed to mark job pending", "job_id", id, "error", err)
		return
	}

	if err := s.q.Enqueue(ctx, j.ID); err != nil {
		j.Status = job.StatusFailed
		j.Error = fmt.Sprintf("enqueueing job: %v", err)
		j.UpdatedAt = time.Now().UTC()
		if uErr := s.jobs.Update(ctx, &j); uErr != nil {
			slog.Error("failed to mark job failed after enqueue error", "job_id", j.ID, "error", uErr)
		}
		if dErr := s.storage.Delete(ctx, j.RawKey); dErr != nil {
			slog.Error("failed to delete raw file after enqueue error", "job_id", j.ID, "error", dErr)
		}
		s.b.Publish(events.Update{
			JobID:    j.ID,
			Status:   string(job.StatusFailed),
			Progress: j.Progress,
		})
		return
	}

	slog.Info("upload validated and queued", "job_id", j.ID, "size", size)
}

// validateVideo streams the stored object to a temp file and probes it.
// True for any video ffprobe understands, regardless of extension.
func (s *Server) validateVideo(ctx context.Context, rawKey string) error {
	obj, err := s.storage.Get(ctx, rawKey)
	if err != nil {
		return fmt.Errorf("fetching upload: %w", err)
	}
	defer obj.Close()

	tmp, err := os.CreateTemp("", "validate-*")
	if err != nil {
		return fmt.Errorf("creating temp file: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)

	if _, err := io.Copy(tmp, obj); err != nil {
		tmp.Close()
		return fmt.Errorf("downloading upload: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("flushing upload: %w", err)
	}

	if _, err := s.tc.Probe(ctx, tmpName); err != nil {
		return err
	}
	return nil
}

// newID generates a random 16-byte hex string as a job ID. Plain
// crypto/rand from the standard library — no need for a UUID dependency
// just for this.
func newID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("newID: %w", err)
	}
	return hex.EncodeToString(b), nil
}
