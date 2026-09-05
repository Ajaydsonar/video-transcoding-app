// Package httpserver
package httpserver

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"video-pipeline/internal/job"
)

// handleGetJob returns a job's current state: status, progress %, error if
// any. This is the polling counterpart to the Server-Sent Events stream
// we'll add once the worker actually exists.
func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id") // Go 1.22 wildcard routing pulls {id} out of "/videos/{id}"

	j, err := s.jobs.Get(r.Context(), id)
	if err != nil {
		// errors.Is unwraps any wrapping (%w chains) to check whether THIS
		// specific sentinel error is anywhere in the chain — more robust
		// than comparing err == job.ErrNotFound directly.
		if errors.Is(err, job.ErrNotFound) {
			writeError(w, http.StatusNotFound, err)
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(j)
}

// handleListVideos returns recently *completed* jobs, newest first —
// the "recent uploads" shelf on the upload page. In-flight jobs live on
// their own detail pages and failed/abandoned ones would just be clutter
// here, so both are filtered out. ?limit=n caps the shelf (default 10,
// max 50).
func (s *Server) handleListVideos(w http.ResponseWriter, r *http.Request) {
	limit := 10
	if q := r.URL.Query().Get("limit"); q != "" {
		n, err := strconv.Atoi(q)
		if err != nil || n <= 0 {
			writeError(w, http.StatusBadRequest, fmt.Errorf("invalid limit %q", q))
			return
		}
		limit = n
	}
	if limit > 50 {
		limit = 50
	}

	// Oversample: the store returns newest-first regardless of status,
	// and filtering to completed may drop entries. Bounded (a few
	// hundred in-memory records), so this stays cheap — a future
	// Postgres store should push the status filter into SQL instead.
	recent, err := s.jobs.ListRecent(r.Context(), limit*5)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	completed := make([]job.Job, 0, limit)
	for _, j := range recent {
		if j.Status == job.StatusCompleted {
			completed = append(completed, j)
		}
		if len(completed) == limit {
			break
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(completed)
}
