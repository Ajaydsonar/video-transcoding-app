package httpserver

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"video-pipeline/internal/events"
	"video-pipeline/internal/job"
)

func (s *Server) handleJobEvents(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("streaming unsupported"))
		return
	}

	// Disable the server-level WriteTimeout for this long-lived SSE connection.
	rc := http.NewResponseController(w)
	if err := rc.SetWriteDeadline(time.Time{}); err != nil {
		slog.Error("failed to disable SSE write deadline", "error", err)
		return
	}

	// Subscribe BEFORE checking current state — if you check state first
	// and subscribe after, a publish landing in that gap is lost forever.
	// Subscribing first means any such publish is already waiting in your
	// buffered channel by the time you get to the loop below.
	ch, unsubscribe := s.b.Subscribe(id)
	defer unsubscribe()

	current, err := s.jobs.Get(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	send := func(u events.Update) {
		data, err := json.Marshal(u)
		if err != nil {
			slog.Error("failed to marshal job update", "job_id", u.JobID, "error", err)
			return
		}

		fmt.Fprintf(w, "data: %s\n\n", data)
		flusher.Flush()
	}

	// Handles the case where the job already finished before this client
	// ever connected — without this, they'd hang forever waiting on ch.
	send(events.Update{JobID: current.ID, Status: string(current.Status), Progress: current.Progress})
	if current.Status == job.StatusCompleted || current.Status == job.StatusFailed {
		return
	}

	for {
		select {
		case update := <-ch:
			send(update)
			if update.Status == string(job.StatusCompleted) || update.Status == string(job.StatusFailed) {
				return
			}
		case <-r.Context().Done():
			return // client disconnected — same pattern as your worker shutdown
		}
	}
}
