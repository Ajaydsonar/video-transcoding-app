package httpserver

import (
	"net/http"
	"strings"
)

// withCORS wraps the mux so browsers hosted on a different origin (e.g.
// the Vercel frontend talking to this API) may call it. Allowlist comes
// from FRONTEND_ORIGIN (comma-separated, configured in config).
//
//   - Simple requests (GET/POST without custom headers, incl. SSE): the
//     origin is echoed back when allowlisted, plus Vary: Origin so any
//     cache in front keys correctly.
//   - Preflight (OPTIONS): answered here with 204 + methods/headers, so
//     no route needs its own OPTIONS handler.
//
// Same-origin callers (curl, vite dev proxy) send no Origin and pass
// through untouched.
func withCORS(next http.Handler, allowedOrigins []string) http.Handler {
	allowed := make(map[string]struct{}, len(allowedOrigins))
	for _, o := range allowedOrigins {
		if o = strings.TrimSpace(o); o != "" {
			allowed[o] = struct{}{}
		}
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin == "" {
			next.ServeHTTP(w, r)
			return
		}
		if _, ok := allowed[origin]; !ok {
			// Unknown origin: serve normally WITHOUT CORS headers. The
			// browser then blocks the read — strictest safe default, and
			// non-browser clients are unaffected.
			next.ServeHTTP(w, r)
			return
		}

		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Vary", "Origin")

		if r.Method == http.MethodOptions {
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Range")
			w.Header().Set("Access-Control-Max-Age", "86400")
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}
