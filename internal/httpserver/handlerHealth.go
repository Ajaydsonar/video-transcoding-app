// Package httpserver ok
package httpserver

import (
	"encoding/json"
	"net/http"
)

// handleHealth is a plain liveness check: if this returns 200, the process
// is up and its config loaded correctly. Later, a /readyz endpoint can
// check downstream deps (DB, storage, queue) separately — liveness and
// readiness answer different questions and shouldn't be conflated.

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	data := map[string]string{
		"status": "ok",
		"env":    s.cfg.Env,
	}

	payload, err := json.Marshal(data)
	if err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(payload)
}
