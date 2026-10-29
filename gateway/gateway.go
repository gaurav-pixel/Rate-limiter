package gateway

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/yourname/rate-limiter-gateway/ratelimiter"
)

// MiddlewareFunc is a standard HTTP middleware signature.
type MiddlewareFunc func(http.Handler) http.Handler

// routeEntry binds a pattern to its handler and an optional per-route limiter.
type routeEntry struct {
	pattern string
	handler http.Handler
	limiter ratelimiter.RateLimiter
	keyFunc KeyFunc
}

// Gateway is a lightweight API gateway that routes requests and enforces
// per-route or global rate limits through a configurable middleware chain.
type Gateway struct {
	mux         *http.ServeMux
	routes      []routeEntry
	middlewares []MiddlewareFunc
	// defaultLimiter is applied to routes that have no explicit limiter.
	defaultLimiter ratelimiter.RateLimiter
	defaultKeyFunc KeyFunc
}

// New creates a new Gateway, registers built-in routes, and wires the default
// rate limiter.
func New(defaultLimiter ratelimiter.RateLimiter, defaultKeyFunc KeyFunc) *Gateway {
	g := &Gateway{
		mux:            http.NewServeMux(),
		defaultLimiter: defaultLimiter,
		defaultKeyFunc: defaultKeyFunc,
	}
	g.registerBuiltinRoutes()
	return g
}

// Use appends a global middleware to the chain. Middlewares are applied in the
// order they are added (outermost first).
func (g *Gateway) Use(mw MiddlewareFunc) {
	g.middlewares = append(g.middlewares, mw)
}

// AddRoute registers a pattern with a handler and an optional per-route
// rate limiter. Pass nil for limiter to use the gateway's default limiter.
func (g *Gateway) AddRoute(pattern string, handler http.Handler, limiter ratelimiter.RateLimiter, keyFunc KeyFunc) {
	if limiter == nil {
		limiter = g.defaultLimiter
	}
	if keyFunc == nil {
		keyFunc = g.defaultKeyFunc
	}

	entry := routeEntry{
		pattern: pattern,
		handler: handler,
		limiter: limiter,
		keyFunc: keyFunc,
	}
	g.routes = append(g.routes, entry)

	// Wrap the handler with its rate limiter and global middlewares.
	wrapped := g.applyMiddlewares(entry)
	g.mux.Handle(pattern, wrapped)
	log.Printf("[gateway] registered route %s", pattern)
}

// applyMiddlewares wraps the route handler with its per-route limiter first,
// then the global middleware chain (outermost last = executes first).
func (g *Gateway) applyMiddlewares(entry routeEntry) http.Handler {
	// Start with the actual handler.
	h := entry.handler

	// Apply the per-route rate limiter (innermost middleware).
	if entry.limiter != nil {
		rlm := NewRateLimitMiddleware(entry.limiter, entry.keyFunc)
		h = rlm.Middleware(h)
	}

	// Apply global middlewares in reverse so the first added is outermost.
	for i := len(g.middlewares) - 1; i >= 0; i-- {
		h = g.middlewares[i](h)
	}
	return h
}

// ServeHTTP satisfies http.Handler and delegates to the internal mux.
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	g.mux.ServeHTTP(w, r)
}

// --- Built-in routes --------------------------------------------------------

func (g *Gateway) registerBuiltinRoutes() {
	// Health check – no rate limiting.
	g.mux.HandleFunc("/health", healthHandler)

	// Functional demo routes are registered via AddRoute so they get rate limited.
}

// RegisterDemoRoutes wires the /api/v1/hello and /api/v1/echo demo endpoints.
// Call this after the Gateway is constructed so the default limiter is in place.
func (g *Gateway) RegisterDemoRoutes() {
	g.AddRoute("/api/v1/hello", http.HandlerFunc(helloHandler), nil, nil)
	g.AddRoute("/api/v1/echo", http.HandlerFunc(echoHandler), nil, nil)
}

// healthHandler returns a simple 200 JSON response used for readiness checks.
func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"status":"ok","time":"%s"}`, time.Now().UTC().Format(time.RFC3339))
}

// helloHandler returns a personalised greeting.
func helloHandler(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	if name == "" {
		name = "world"
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"message":"hello, %s!","time":"%s"}`, name, time.Now().UTC().Format(time.RFC3339))
}

// echoHandler echoes the request body back to the caller (up to 1 MB).
func echoHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, `{"error":"failed to read body"}`, http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	// Try to pretty-print if JSON.
	var js json.RawMessage
	if json.Unmarshal(body, &js) == nil {
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	w.Write(body)
}
