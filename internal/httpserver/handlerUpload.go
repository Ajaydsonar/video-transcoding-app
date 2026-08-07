package httpserver

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"video-pipeline/internal/job"
)

// maxInMemory is how much of the multipart body Go buffers in RAM before
// spilling the rest to a temp file on disk. It does NOT cap upload size —
// a 5GB video is fine, only the first 32MB touches memory.
const maxInMemory = 32 << 20 // 32MB

// handleUpload accepts a multipart video upload, stores the raw bytes via
// the Storage interface, and creates a Job record to track it. It returns
// immediately (202 Accepted) with a job ID — the actual transcoding
// happens asynchronously in a later step, and the client polls or streams
// progress instead of blocking on this request.
func (s *Server) handleUpload(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseMultipartForm(maxInMemory); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("parsing upload: %w", err))
		return
	}

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

	rawKey := fmt.Sprintf("raw/%s/%s", id, header.Filename)

	// r.Context() is cancelled if the client disconnects mid-upload — Put
	// receives that via ctx and (once storage/r2.go exists) can abort the
	// in-flight write instead of finishing a copy nobody wants anymore.
	if err := s.storage.Put(r.Context(), rawKey, file); err != nil {
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
		writeError(w, http.StatusInternalServerError, fmt.Errorf("enque job record: %w", err))
		return
	}

	slog.Info("upload accepted", "job_id", id, "filename", header.Filename)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(j)
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
