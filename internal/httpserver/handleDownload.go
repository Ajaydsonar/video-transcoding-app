package httpserver

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"video-pipeline/internal/job"
)

func (s *Server) handleDownload(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	name := r.PathValue("name")

	j, err := s.jobs.Get(r.Context(), id)
	if err != nil {
		if errors.Is(err, job.ErrNotFound) {
			writeError(w, http.StatusNotFound, err)
			return
		}
		writeError(w, http.StatusInternalServerError, fmt.Errorf("loading job: %w", err))
		return
	}

	// This endpoint only serves MP4 renditions. HLS jobs are packaged
	// as playlist + segment trees — fetch those from /videos/{id}/hls/.
	if j.Kind != job.KindMP4 {
		writeError(w, http.StatusNotFound, fmt.Errorf("job %s has no mp4 renditions (kind %q), use /videos/%s/hls/ instead", id, j.Kind, id))
		return
	}

	// Large video downloads easily outlive the server's 15s WriteTimeout,
	// so disable the write deadline for this response.
	rc := http.NewResponseController(w)
	if err := rc.SetWriteDeadline(time.Time{}); err != nil {
		slog.Warn("failed to disable download write deadline", "error", err)
	}
	// SECURITY: never build the storage key directly from the URL param.
	// `name` came straight from the client — validate it against this
	// job's ACTUAL Outputs first. Otherwise a crafted request could probe
	// for arbitrary keys under processed/<id>/... that were never really
	// produced (or, if key-building were sloppier elsewhere, worse).

	var target *job.Output

	for i := range j.Outputs {
		if j.Outputs[i].Name == name {
			target = &j.Outputs[i]
			break
		}
	}

	if target == nil {
		writeError(w, http.StatusNotFound, fmt.Errorf("no such rendition %q", name))
		return
	}

	key := fmt.Sprintf("processed/%s/%s.mp4", id, name)
	reader, err := s.storage.Get(r.Context(), key)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("err reading file"))
		return
	}
	defer reader.Close()

	w.Header().Set("Content-Type", "video/mp4")
	w.Header().Set("Content-Length", strconv.FormatInt(target.SizeBytes, 10))
	io.Copy(w, reader)
}
