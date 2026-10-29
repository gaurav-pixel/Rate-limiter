package config

import "time"

// Algorithm represents the rate limiting algorithm to use
type Algorithm string

const (
	AlgorithmTokenBucket   Algorithm = "token_bucket"
	AlgorithmSlidingWindow Algorithm = "sliding_window"
)

// RateLimitConfig holds configuration for a single rate limiting policy
type RateLimitConfig struct {
	// Algorithm selects the rate limiting strategy
	Algorithm Algorithm `json:"algorithm"`
	// Limit is the maximum number of requests allowed in the window
	Limit int `json:"limit"`
	// Window is the time window for the sliding window algorithm
	Window time.Duration `json:"window"`
	// BurstSize is the maximum burst capacity for token bucket
	BurstSize int `json:"burst_size"`
	// RefillRate is the token replenishment rate per second for token bucket
	RefillRate float64 `json:"refill_rate"`
}

// RouteConfig defines rate limiting policy for a specific route
type RouteConfig struct {
	// Pattern is the URL path pattern
	Pattern string `json:"pattern"`
	// Policy is the rate limiting policy for this route
	Policy RateLimitConfig `json:"policy"`
	// KeyFunc determines how to extract the rate limit key from a request
	// Supported values: "ip", "header:<name>", "path"
	KeyFunc string `json:"key_func"`
}

// RedisConfig holds Redis connection configuration
type RedisConfig struct {
	// Addr is the Redis server address (host:port)
	Addr string `json:"addr"`
	// Password is the Redis authentication password
	Password string `json:"password"`
	// DB is the Redis database number
	DB int `json:"db"`
	// PoolSize is the number of connections in the pool
	PoolSize int `json:"pool_size"`
	// MaxRetries is the maximum number of retries on failure
	MaxRetries int `json:"max_retries"`
}

// ServerConfig holds HTTP server configuration
type ServerConfig struct {
	// Port is the port the server listens on
	Port int `json:"port"`
	// ReadTimeout is the maximum duration for reading the request
	ReadTimeout time.Duration `json:"read_timeout"`
	// WriteTimeout is the maximum duration before timing out writes
	WriteTimeout time.Duration `json:"write_timeout"`
	// IdleTimeout is the maximum time to wait for the next request
	IdleTimeout time.Duration `json:"idle_timeout"`
	// ShutdownTimeout is the maximum time to wait for graceful shutdown
	ShutdownTimeout time.Duration `json:"shutdown_timeout"`
}

// GatewayConfig is the top-level configuration for the API gateway
type GatewayConfig struct {
	// Routes defines per-route rate limiting policies
	Routes []RouteConfig `json:"routes"`
	// DefaultPolicy is the fallback rate limiting policy
	DefaultPolicy RateLimitConfig `json:"default_policy"`
	// Redis is the Redis connection configuration (optional)
	Redis *RedisConfig `json:"redis,omitempty"`
	// Server is the HTTP server configuration
	Server ServerConfig `json:"server"`
}

// DefaultConfig returns a sensible default configuration
func DefaultConfig() *GatewayConfig {
	return &GatewayConfig{
		DefaultPolicy: RateLimitConfig{
			Algorithm:  AlgorithmTokenBucket,
			Limit:      100,
			Window:     time.Minute,
			BurstSize:  20,
			RefillRate: 10,
		},
		Routes: []RouteConfig{
			{
				Pattern: "/api/v1/hello",
				Policy: RateLimitConfig{
					Algorithm:  AlgorithmTokenBucket,
					Limit:      60,
					Window:     time.Minute,
					BurstSize:  10,
					RefillRate: 1,
				},
				KeyFunc: "ip",
			},
			{
				Pattern: "/api/v1/echo",
				Policy: RateLimitConfig{
					Algorithm: AlgorithmSlidingWindow,
					Limit:     30,
					Window:    time.Minute,
				},
				KeyFunc: "ip",
			},
		},
		Server: ServerConfig{
			Port:            8080,
			ReadTimeout:     15 * time.Second,
			WriteTimeout:    15 * time.Second,
			IdleTimeout:     60 * time.Second,
			ShutdownTimeout: 30 * time.Second,
		},
	}
}
