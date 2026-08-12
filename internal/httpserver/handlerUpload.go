package httpserver

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"video-pipeline/internal/job"
)

const (
	maxInMemory   = 32 << 20 // 32MB kept in memory before multipart spills to a temp file on disk
	maxUploadSize = 2 << 30  // 2GB hard cap — anything beyond this, we stop reading, not just reject after receiving it all
	maxQueueDepth = 20       // beyond this many pending jobs, refuse new uploads with 503 rather than queue indefinitely
)

// handleUpload accepts a multipart video upload, validates it's really a
// video, stores the raw bytes, and creates a Job record to track it. It
// returns immediately (202 Accepted) with a job ID — transcoding happens
// asynchronously, and the client polls or streams progress instead of
// blocking on this request.
func (s *Server) handleUpload(w http.ResponseWriter, r *http.Request) {
	// Cheapest possible check, first: if we're already backed up, refuse
	// before spending a single byte of read, disk, or CPU on this
	// request. A client should hear "try again shortly," not have their
	// upload hang while it silently queues behind hundreds of others.
	if s.q.Len() >= maxQueueDepth {
		w.Header().Set("Retry-After", "30")
		writeError(w, http.StatusServiceUnavailable, fmt.Errorf("server is at capacity, try again shortly"))
		return
	}

	// Disable the server-level WriteTimeout for this long-lived SSE connection.
	rc := http.NewResponseController(w)
	if err := rc.SetWriteDeadline(time.Time{}); err != nil {
		slog.Error("failed to disable SSE write deadline", "error", err)
		return
	}

	// MaxBytesReader stops reading (and makes ParseMultipartForm/FormFile
	// fail) the instant the body exceeds this many bytes — it protects us
	// WHILE reading, not just after the fact. It also requires us to set
	// w's underlying connection to close on overflow, which the stdlib
	// handles internally here.
	r.Body = http.MaxBytesReader(w, r.Body, maxUploadSize)

	if err := r.ParseMultipartForm(maxInMemory); err != nil {
		if err.Error() == "http: request body too large" {
			writeError(w, http.StatusRequestEntityTooLarge, fmt.Errorf("upload exceeds %d byte limit", maxUploadSize))
			return
		}
		writeError(w, http.StatusBadRequest, fmt.Errorf("parsing upload: %w", err))
		return
	}
	defer r.MultipartForm.RemoveAll()

	file, header, err := r.FormFile("video")
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("missing 'video' field: %w", err))
		return
	}
	defer file.Close()

	id, err := newID()
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("generating job id: %w", err))
		return
	}

	// Save locally first — we need a real file path to validate with
	// ffprobe (it can't probe an in-flight multipart stream), and this
	// also means a garbage upload never touches Storage or the queue at
	// all: reject cheap, before spending any real resource on it.
	tmpPath, err := saveTemp(file)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("saving upload: %w", err))
		return
	}
	defer os.Remove(tmpPath)

	if _, err := s.tc.Probe(r.Context(), tmpPath); err != nil {
		slog.Warn("upload rejected: not a valid video", "filename", header.Filename, "error", err)
		writeError(w, http.StatusBadRequest, fmt.Errorf("file doesn't look like a valid video: %w", err))
		return
	}

	tmpFile, err := os.Open(tmpPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("reopening upload: %w", err))
		return
	}
	defer tmpFile.Close()

	name := filepath.Base(strings.ReplaceAll(header.Filename, "\\", "/"))
	if name == "." || name == ".." || name == "/" || name == "" {
		name = "upload" // safe fallback
	}
	// optionally strip control characters / non-ASCII for stricter hygiene
	rawKey := fmt.Sprintf("raw/%s/%s", id, name)

	// rawKey := fmt.Sprintf("raw/%s/%s", id, header.Filename)
	if err := s.storage.Put(r.Context(), rawKey, tmpFile); err != nil {
		slog.Error("failed to store upload", "job_id", id, "error", err)
		writeError(w, http.StatusInternalServerError, fmt.Errorf("storing upload: %w", err))
		return
	}

	now := time.Now().UTC()
	j := &job.Job{
		ID:        id,
		Status:    job.StatusPending,
		RawKey:    rawKey,
		CreatedAt: now,
		UpdatedAt: now,
	}

	if err := s.jobs.Create(r.Context(), j); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("creating job record: %w", err))
		return
	}

	if err := s.q.Enqueue(r.Context(), j.ID); err != nil {
		// Enqueue fails most often because the client disconnected, which
		// means r.Context() is already cancelled — never reuse it for
		// cleanup. Use a fresh background context with a timeout instead.
		cleanCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		j.Status = job.StatusFailed
		if uErr := s.jobs.Update(cleanCtx, j); uErr != nil {
			slog.Error("failed to mark job failed after enqueue error", "job_id", j.ID, "error", uErr)
		}
		if dErr := s.storage.Delete(cleanCtx, j.RawKey); dErr != nil {
			slog.Error("failed to delete raw file after enqueue error", "job_id", j.ID, "error", dErr)
		}

		writeError(w, http.StatusInternalServerError, fmt.Errorf("enqueueing job: %w", err))
		return
	}

	slog.Info("upload accepted", "job_id", id, "filename", header.Filename)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(j)
}

// saveTemp copies an uploaded file to a real path on disk so ffprobe has
// something to point at. errors.As isn't used here, but MaxBytesReader's
// overflow surfaces as a plain error from the copy, same as any other I/O
// failure — the handler above already checked the size limit earlier at
// the ParseMultipartForm stage, so this mainly guards against disk issues.
func saveTemp(r io.Reader) (string, error) {
	f, err := os.CreateTemp("", "upload-*")
	if err != nil {
		return "", err
	}
	defer f.Close()

	if _, err := io.Copy(f, r); err != nil {
		os.Remove(f.Name())
		return "", err
	}
	return f.Name(), nil
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
