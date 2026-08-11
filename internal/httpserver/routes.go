package httpserver

import (
	"time"

	"golang.org/x/time/rate"
)

// routes registers every endpoint the server exposes. Keeping this in one
// place (separate from server.go) means you can see the entire API surface
// without reading handler implementations.
//
// Go 1.22 added method + wildcard matching directly to net/http.ServeMux
// (e.g. "GET /jobs/{id}"), so we don't need a third-party router for a
// service this size.
func (s *Server) routes() {
	// Separate budgets per endpoint: uploads kick off real CPU work and
	// get a tight allowance; status checks are a cheap map read and can
	// be generous. One shared limiter for both would force a tradeoff
	// neither endpoint actually needs.
	uploadLimiter := newIPLimiter(rate.Every(10*time.Second), 3) // ~1 upload/10s sustained, bursts of 3
	statusLimiter := newIPLimiter(rate.Limit(5), 20)             // 5 req/s sustained, bursts of 20

	s.mux.HandleFunc("GET /healthz", s.handleHealth)
	s.mux.HandleFunc("POST /videos", rateLimit(uploadLimiter, s.handleUpload))
	s.mux.HandleFunc("GET /videos/{id}/events", s.handleJobEvents)
	s.mux.HandleFunc("GET /videos/{id}/file/{name}", s.handleDownload)
	s.mux.HandleFunc("GET /videos/{id}", rateLimit(statusLimiter, s.handleGetJob))
}