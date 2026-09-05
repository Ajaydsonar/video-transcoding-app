package httpserver

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"path"
	"strings"
	"time"

	"video-pipeline/internal/job"
)

// hlsContentTypes maps HLS tree extensions to their wire types. The .m3u8
// type must be exact — players refuse playlists served as text/plain or
// octet-stream.
var hlsContentTypes = map[string]string{
	".m3u8": "application/vnd.apple.mpegurl",
	".m4s":  "video/iso.segment",
	".mp4":  "video/mp4",
}

// handleHLSFile serves files from an HLS job's packaged tree: the master
// playlist, variant playlists, init + media segments, and the shared
// audio playlist.
//
// URL:  GET /videos/{id}/hls/<path>
// Key:  processed/<id>/hls/<path>
//
// Segments are immutable once written, so they can be cached by players —
// playlists are served no-cache since the tree grows while the job runs.
func (s *Server) handleHLSFile(w http.ResponseWriter, r *http.Request) {
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

	if j.Kind != job.KindHLS {
		writeError(w, http.StatusNotFound, fmt.Errorf("job %s is not an HLS job (kind %q)", id, j.Kind))
		return
	}

	// Media segments easily outlive the server's 15s WriteTimeout, so
	// disable the write deadline for this response.
	rc := http.NewResponseController(w)
	if err := rc.SetWriteDeadline(time.Time{}); err != nil {
		slog.Warn("failed to disable HLS write deadline", "error", err)
	}

	// The mux matches the "/videos/{id}/hls/" prefix; the remainder is
	// the path inside the HLS tree. Never trust it raw — clean it and
	// reject anything that escapes the job's own prefix.
	rest, ok := strings.CutPrefix(r.URL.Path, "/videos/"+id+"/hls/")
	if !ok || rest == "" {
		writeError(w, http.StatusNotFound, fmt.Errorf("missing HLS file path"))
		return
	}
	clean := path.Clean(rest)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") ||
		strings.Contains(clean, "\\") || path.IsAbs(clean) {
		writeError(w, http.StatusBadRequest, fmt.Errorf("invalid HLS file path %q", rest))
		return
	}

	key := "processed/" + id + "/hls/" + clean

	reader, err := s.storage.Get(r.Context(), key)
	if err != nil {
		writeError(w, http.StatusNotFound, fmt.Errorf("no such HLS file %q", clean))
		return
	}
	defer reader.Close()

	contentType, ok := hlsContentTypes[strings.ToLower(path.Ext(clean))]
	if !ok {
		contentType = "application/octet-stream"
	}
	w.Header().Set("Content-Type", contentType)
	if strings.HasSuffix(strings.ToLower(clean), ".m3u8") {
		w.Header().Set("Cache-Control", "no-cache")
	}
	io.Copy(w, reader)
}
