// Package httpserver
package httpserver

import (
	"encoding/json"
	"errors"
	"net/http"

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
