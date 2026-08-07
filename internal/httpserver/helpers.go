package httpserver

import (
	"encoding/json"
	"log/slog"
	"net/http"
)

// writeError sends a consistent {"error": "..."} JSON body and logs the
// failure. One place for this means every handler fails the same way —
// no handler forgets to set Content-Type or log the error.
func writeError(w http.ResponseWriter, status int, err error) {
	slog.Warn("request failed", "status", status, "error", err)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}
