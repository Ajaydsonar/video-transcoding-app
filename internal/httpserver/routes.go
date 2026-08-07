package httpserver

// routes registers every endpoint the server exposes. Keeping this in one
// place (separate from server.go) means you can see the entire API surface
// without reading handler implementations.
//
// Go 1.22 added method + wildcard matching directly to net/http.ServeMux
// (e.g. "GET /jobs/{id}"), so we don't need a third-party router for a
// service this size.
func (s *Server) routes() {
	s.mux.HandleFunc("GET /healthz", s.handleHealth)
	s.mux.HandleFunc("POST /videos", s.handleUpload)
	s.mux.HandleFunc("GET /videos/{id}", s.handleGetJob)
	s.mux.HandleFunc("GET /videos/{id}/events", s.handleJobEvents)
}
