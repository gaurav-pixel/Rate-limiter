package ratelimiter

import "context"

// RateLimiter is the common interface for all rate limiter implementations.
type RateLimiter interface {
	// Allow checks if a single request from the given key is allowed.
	Allow(ctx context.Context, key string) (bool, error)
	// AllowN checks if n requests from the given key are allowed atomically.
	AllowN(ctx context.Context, key string, n int) (bool, error)
	// Reset resets the rate limit state for the given key.
	Reset(ctx context.Context, key string) error
	// Close releases any resources held by the limiter.
	Close() error
}

// Result holds detailed information about a rate limit check.
type Result struct {
	// Allowed indicates whether the request(s) were permitted.
	Allowed bool
	// Remaining is the number of requests still allowed in the current window.
	Remaining int
	// ResetAfter is milliseconds until the limit resets (token bucket: time to full).
	ResetAfter int64
	// RetryAfter is milliseconds until a rejected request may be retried.
	RetryAfter int64
}
