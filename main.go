package main

import (
	"flag"
	"log"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/yourname/rate-limiter-gateway/config"
	"github.com/yourname/rate-limiter-gateway/gateway"
	"github.com/yourname/rate-limiter-gateway/ratelimiter"
	"github.com/yourname/rate-limiter-gateway/server"
)

func main() {
	// ---------------------------------------------------------------------------
	// CLI flags
	// ---------------------------------------------------------------------------
	redisAddr := flag.String("redis-addr", "", "Redis address (host:port). Leave empty for in-memory limiters.")
	redisPassword := flag.String("redis-password", "", "Redis password (optional).")
	algorithm := flag.String("algorithm", "token_bucket", "Rate limiting algorithm: token_bucket | sliding_window")
	limit := flag.Int("limit", 100, "Maximum requests per window (sliding_window) or bucket capacity (token_bucket)")
	window := flag.Duration("window", time.Minute, "Time window for sliding_window algorithm (e.g. 1m, 30s)")
	burst := flag.Int("burst", 20, "Burst size / capacity for token_bucket")
	refillRate := flag.Float64("refill-rate", 10, "Token refill rate per second (token_bucket only)")
	port := flag.Int("port", 8080, "HTTP port to listen on")
	flag.Parse()

	// ---------------------------------------------------------------------------
	// Build the rate limiter
	// ---------------------------------------------------------------------------
	var limiter ratelimiter.RateLimiter

	if *redisAddr != "" {
		// Redis-backed limiter
		rdb := redis.NewClient(&redis.Options{
			Addr:     *redisAddr,
			Password: *redisPassword,
			DB:       0,
		})
		log.Printf("[main] using Redis at %s", *redisAddr)

		switch config.Algorithm(*algorithm) {
		case config.AlgorithmSlidingWindow:
			limiter = ratelimiter.NewRedisSlidingWindow(rdb, *limit, *window)
			log.Printf("[main] algorithm: redis sliding_window  limit=%d  window=%s", *limit, *window)
		default:
			limiter = ratelimiter.NewRedisTokenBucket(rdb, float64(*burst), *refillRate)
			log.Printf("[main] algorithm: redis token_bucket  capacity=%d  refill=%.1f/s", *burst, *refillRate)
		}
	} else {
		// In-memory limiter
		log.Printf("[main] no Redis address provided – using in-memory limiter")

		switch config.Algorithm(*algorithm) {
		case config.AlgorithmSlidingWindow:
			limiter = ratelimiter.NewSlidingWindow(*limit, *window)
			log.Printf("[main] algorithm: in-memory sliding_window  limit=%d  window=%s", *limit, *window)
		default:
			limiter = ratelimiter.NewTokenBucket(float64(*burst), *refillRate)
			log.Printf("[main] algorithm: in-memory token_bucket  capacity=%d  refill=%.1f/s", *burst, *refillRate)
		}
	}

	defer func() {
		if err := limiter.Close(); err != nil {
			log.Printf("[main] error closing limiter: %v", err)
		}
	}()

	// ---------------------------------------------------------------------------
	// Build gateway
	// ---------------------------------------------------------------------------
	gw := gateway.New(limiter, gateway.IPKeyFunc)

	// Global request logger middleware
	gw.Use(loggingMiddleware)

	// Register demo routes (they inherit the default limiter)
	gw.RegisterDemoRoutes()

	// ---------------------------------------------------------------------------
	// Start server
	// ---------------------------------------------------------------------------
	cfg := config.ServerConfig{
		Port:            *port,
		ReadTimeout:     15 * time.Second,
		WriteTimeout:    15 * time.Second,
		IdleTimeout:     60 * time.Second,
		ShutdownTimeout: 30 * time.Second,
	}

	srv := server.New(cfg, gw)
	if err := srv.Start(); err != nil {
		log.Fatalf("[main] server exited with error: %v", err)
	}
}

// loggingMiddleware logs every incoming request with method, path and remote addr.
func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Printf("[request] %s %s %s", r.Method, r.URL.Path, r.RemoteAddr)
		next.ServeHTTP(w, r)
	})
}
