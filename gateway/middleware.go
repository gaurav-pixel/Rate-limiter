package gateway

import (
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/yourname/rate-limiter-gateway/ratelimiter"
)

// KeyFunc extracts a rate limit key from an HTTP request.
type KeyFunc func(r *http.Request) string

// IPKeyFunc extracts the client IP address as the rate limit key.
func IPKeyFunc(r *http.Request) string {
	// Respect X-Forwarded-For when behind a proxy.
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.SplitN(xff, ",", 2)
		ip := strings.TrimSpace(parts[0])
		if ip != "" {
			return ip
		}
	}
	// Fall back to X-Real-IP.
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return strings.TrimSpace(xri)
	}
	// Strip port from RemoteAddr.
	addr := r.RemoteAddr
	if idx := strings.LastIndex(addr, ":"); idx != -1 {
		addr = addr[:idx]
	}
	return addr
}

// HeaderKeyFunc returns a KeyFunc that uses the value of the named header.
func HeaderKeyFunc(headerName string) KeyFunc {
	return func(r *http.Request) string {
		v := r.Header.Get(headerName)
		if v == "" {
			return "anonymous"
		}
		return v
	}
}

// PathKeyFunc uses the request URL path as the rate limit key.
func PathKeyFunc(r *http.Request) string {
	return r.URL.Path
}

// BuildKeyFunc converts the string key-func name used in RouteConfig into a KeyFunc.
func BuildKeyFunc(spec string) KeyFunc {
	switch {
	case spec == "ip" || spec == "":
		return IPKeyFunc
	case spec == "path":
		return PathKeyFunc
	case strings.HasPrefix(spec, "header:"):
		name := strings.TrimPrefix(spec, "header:")
		return HeaderKeyFunc(name)
	default:
		return IPKeyFunc
	}
}

// RateLimitMiddleware applies a RateLimiter to incoming HTTP requests.
type RateLimitMiddleware struct {
	limiter ratelimiter.RateLimiter
	keyFunc KeyFunc
}

// NewRateLimitMiddleware creates a new RateLimitMiddleware.
func NewRateLimitMiddleware(limiter ratelimiter.RateLimiter, keyFunc KeyFunc) *RateLimitMiddleware {
	return &RateLimitMiddleware{
		limiter: limiter,
		keyFunc: keyFunc,
	}
}

// Middleware wraps next with rate limiting logic.
//
// On rejection it responds with:
//   - HTTP 429 Too Many Requests
//   - Retry-After header (seconds)
//   - X-RateLimit-Remaining header
//   - X-RateLimit-Reset header (Unix timestamp)
func (m *RateLimitMiddleware) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := m.keyFunc(r)

		allowed, err := m.limiter.Allow(r.Context(), key)
		if err != nil {
			// Log the error but fail open – don't block traffic on limiter errors.
			log.Printf("[rate-limiter] error checking limit for key %q: %v", key, err)
			next.ServeHTTP(w, r)
			return
		}

		if !allowed {
			// Compute a simple Retry-After: 1 second is a safe default.
			retryAfter := 1
			resetAt := time.Now().Add(time.Second).Unix()

			w.Header().Set("X-RateLimit-Remaining", "0")
			w.Header().Set("X-RateLimit-Reset", fmt.Sprintf("%d", resetAt))
			w.Header().Set("Retry-After", fmt.Sprintf("%d", retryAfter))
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			fmt.Fprintf(w, `{"error":"rate limit exceeded","retry_after":%d}`, retryAfter)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// Wrap is a convenience helper – equivalent to m.Middleware(next).
func (m *RateLimitMiddleware) Wrap(next http.Handler) http.Handler {
	return m.Middleware(next)
}
