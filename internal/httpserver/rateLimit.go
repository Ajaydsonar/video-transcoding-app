package httpserver

import (
	"fmt"
	"net"
	"net/http"
	"sync"

	"golang.org/x/time/rate"
)

// ipLimiter hands out one token-bucket rate limiter per client IP,
// creating them lazily on first request. A token bucket refills at a
// steady rate and allows short bursts up to its capacity — smoother than
// a hard "N requests per minute" window, and cheap to check.
type ipLimiter struct {
	mu       sync.Mutex
	limiters map[string]*rate.Limiter
	r        rate.Limit // sustained requests/sec allowed, per IP
	burst    int        // how many requests can fire back-to-back before throttling kicks in
}

func newIPLimiter(r rate.Limit, burst int) *ipLimiter {
	return &ipLimiter{
		limiters: make(map[string]*rate.Limiter),
		r:        r,
		burst:    burst,
	}
}

func (l *ipLimiter) allow(ip string) bool {
	l.mu.Lock()
	lim, ok := l.limiters[ip]
	if !ok {
		lim = rate.NewLimiter(l.r, l.burst)
		l.limiters[ip] = lim
	}
	l.mu.Unlock()
	// rate.Limiter is itself safe for concurrent use — no need to hold
	// l.mu while calling Allow, only while touching the map.
	return lim.Allow()
}

// rateLimit wraps a handler so requests over the limit get 429 instead of
// reaching the real handler at all. Takes a *separate* limiter per route,
// since /videos (expensive, kicks off real work) and /videos/{id}
// (cheap, just a status read) should have very different budgets.
func rateLimit(limiter *ipLimiter, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ip := clientIP(r)
		if !limiter.allow(ip) {
			w.Header().Set("Retry-After", "1")
			writeError(w, http.StatusTooManyRequests, fmt.Errorf("rate limit exceeded, slow down"))
			return
		}
		next(w, r)
	}
}

// clientIP extracts just the IP from RemoteAddr (which is "ip:port").
// NOTE: if this ever sits behind a reverse proxy/CDN, RemoteAddr will be
// the PROXY's address, not the real client — you'd need to read
// X-Forwarded-For instead, and only trust it because you control (and
// therefore trust) what's immediately in front of you.
func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr // fall back to the raw value rather than fail the request over this
	}
	return host
}
