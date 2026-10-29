package ratelimiter

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"
)

// bucket holds the per-key state for the token bucket algorithm.
type bucket struct {
	tokens     float64
	lastRefill time.Time
}

// TokenBucket implements an in-memory token bucket rate limiter.
//
// The bucket starts full (capacity tokens). Tokens are consumed on each
// allowed request and continuously replenished at refillRate tokens/second
// up to capacity. Requests that would exceed the available tokens are denied.
type TokenBucket struct {
	capacity   float64
	refillRate float64 // tokens per second
	buckets    map[string]*bucket
	mu         sync.Mutex
}

// NewTokenBucket creates a new in-memory token bucket limiter.
//
//   - capacity   – maximum tokens the bucket can hold (also the initial fill)
//   - refillRate – tokens added per second
func NewTokenBucket(capacity float64, refillRate float64) *TokenBucket {
	return &TokenBucket{
		capacity:   capacity,
		refillRate: refillRate,
		buckets:    make(map[string]*bucket),
	}
}

// refill adds tokens to b based on the elapsed time since the last refill.
// Must be called with tb.mu held.
func (tb *TokenBucket) refill(b *bucket, now time.Time) {
	elapsed := now.Sub(b.lastRefill).Seconds()
	b.tokens = math.Min(tb.capacity, b.tokens+elapsed*tb.refillRate)
	b.lastRefill = now
}

// getBucket returns the bucket for key, creating it if necessary.
// Must be called with tb.mu held.
func (tb *TokenBucket) getBucket(key string) *bucket {
	b, ok := tb.buckets[key]
	if !ok {
		b = &bucket{
			tokens:     tb.capacity,
			lastRefill: time.Now(),
		}
		tb.buckets[key] = b
	}
	return b
}

// Allow checks whether a single request for key is permitted.
func (tb *TokenBucket) Allow(_ context.Context, key string) (bool, error) {
	return tb.AllowN(context.Background(), key, 1)
}

// AllowN checks whether n requests for key are permitted atomically.
func (tb *TokenBucket) AllowN(_ context.Context, key string, n int) (bool, error) {
	if n <= 0 {
		return false, fmt.Errorf("n must be positive, got %d", n)
	}

	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := time.Now()
	b := tb.getBucket(key)
	tb.refill(b, now)

	requested := float64(n)
	if b.tokens >= requested {
		b.tokens -= requested
		return true, nil
	}
	return false, nil
}

// Reset clears the token bucket for the given key.
func (tb *TokenBucket) Reset(_ context.Context, key string) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	delete(tb.buckets, key)
	return nil
}

// Close is a no-op for the in-memory implementation.
func (tb *TokenBucket) Close() error {
	return nil
}

// Remaining returns the current token count for key without consuming any tokens.
func (tb *TokenBucket) Remaining(key string) float64 {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	b, ok := tb.buckets[key]
	if !ok {
		return tb.capacity
	}
	tb.refill(b, time.Now())
	return b.tokens
}
