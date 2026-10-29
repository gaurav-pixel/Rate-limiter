package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/yourname/rate-limiter-gateway/ratelimiter"
)

// ---- sequential benchmarks --------------------------------------------------

// BenchmarkTokenBucketAllow measures single-goroutine Allow throughput for the
// in-memory token bucket.
func BenchmarkTokenBucketAllow(b *testing.B) {
	limiter := ratelimiter.NewTokenBucket(float64(b.N)+1, float64(b.N)+1)
	defer limiter.Close()

	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		limiter.Allow(ctx, "bench-key") //nolint:errcheck
	}
}

// BenchmarkSlidingWindowAllow measures single-goroutine Allow throughput for
// the in-memory sliding window.
func BenchmarkSlidingWindowAllow(b *testing.B) {
	limiter := ratelimiter.NewSlidingWindow(b.N+1, time.Hour)
	defer limiter.Close()

	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		limiter.Allow(ctx, "bench-key") //nolint:errcheck
	}
}

// ---- parallel benchmarks ----------------------------------------------------

// BenchmarkTokenBucketAllowParallel measures concurrent Allow throughput for
// the in-memory token bucket across GOMAXPROCS goroutines.
func BenchmarkTokenBucketAllowParallel(b *testing.B) {
	limiter := ratelimiter.NewTokenBucket(float64(b.N)*100, float64(b.N)*100)
	defer limiter.Close()

	ctx := context.Background()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			limiter.Allow(ctx, "bench-key") //nolint:errcheck
		}
	})
}

// BenchmarkSlidingWindowAllowParallel measures concurrent Allow throughput for
// the in-memory sliding window across GOMAXPROCS goroutines.
func BenchmarkSlidingWindowAllowParallel(b *testing.B) {
	limiter := ratelimiter.NewSlidingWindow(b.N*100, time.Hour)
	defer limiter.Close()

	ctx := context.Background()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			limiter.Allow(ctx, "bench-key") //nolint:errcheck
		}
	})
}

// ---- per-key parallel benchmarks --------------------------------------------

// BenchmarkTokenBucketAllowParallelPerKey is like BenchmarkTokenBucketAllowParallel
// but each goroutine uses its own key (tests map contention, not per-bucket contention).
func BenchmarkTokenBucketAllowParallelPerKey(b *testing.B) {
	limiter := ratelimiter.NewTokenBucket(float64(b.N)*100, float64(b.N)*100)
	defer limiter.Close()

	ctx := context.Background()
	var id int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		key := fmt.Sprintf("key-%d", &id) // unique per goroutine
		for pb.Next() {
			limiter.Allow(ctx, key) //nolint:errcheck
		}
	})
}

// BenchmarkSlidingWindowAllowParallelPerKey tests the sliding window under
// per-goroutine key isolation.
func BenchmarkSlidingWindowAllowParallelPerKey(b *testing.B) {
	limiter := ratelimiter.NewSlidingWindow(b.N*100, time.Hour)
	defer limiter.Close()

	ctx := context.Background()
	var id int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		key := fmt.Sprintf("key-%d", &id)
		for pb.Next() {
			limiter.Allow(ctx, key) //nolint:errcheck
		}
	})
}

// ---- Redis benchmarks -------------------------------------------------------

// BenchmarkRedisTokenBucketAllow benchmarks the Redis token bucket.
// Skipped automatically when Redis is unavailable.
func BenchmarkRedisTokenBucketAllow(b *testing.B) {
	rdb := redisClient()
	if rdb == nil {
		b.Skip("Redis not available – set REDIS_ADDR or start Redis on localhost:6379")
	}
	defer rdb.Close()

	limiter := ratelimiter.NewRedisTokenBucket(rdb, float64(b.N)+1, float64(b.N)+1)
	defer limiter.Close()

	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		limiter.Allow(ctx, "bench-key") //nolint:errcheck
	}
}

// BenchmarkRedisSlidingWindowAllow benchmarks the Redis sliding window.
// Skipped automatically when Redis is unavailable.
func BenchmarkRedisSlidingWindowAllow(b *testing.B) {
	rdb := redisClient()
	if rdb == nil {
		b.Skip("Redis not available – set REDIS_ADDR or start Redis on localhost:6379")
	}
	defer rdb.Close()

	limiter := ratelimiter.NewRedisSlidingWindow(rdb, b.N+1, time.Hour)
	defer limiter.Close()

	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		limiter.Allow(ctx, "bench-key") //nolint:errcheck
	}
}

// BenchmarkRedisTokenBucketAllowParallel benchmarks the Redis token bucket with
// concurrent goroutines.
func BenchmarkRedisTokenBucketAllowParallel(b *testing.B) {
	rdb := redisClient()
	if rdb == nil {
		b.Skip("Redis not available – set REDIS_ADDR or start Redis on localhost:6379")
	}
	defer rdb.Close()

	limiter := ratelimiter.NewRedisTokenBucket(rdb, float64(b.N)*100, float64(b.N)*100)
	defer limiter.Close()

	ctx := context.Background()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			limiter.Allow(ctx, "bench-key") //nolint:errcheck
		}
	})
}

// BenchmarkRedisSlidingWindowAllowParallel benchmarks the Redis sliding window
// with concurrent goroutines.
func BenchmarkRedisSlidingWindowAllowParallel(b *testing.B) {
	rdb := redisClient()
	if rdb == nil {
		b.Skip("Redis not available – set REDIS_ADDR or start Redis on localhost:6379")
	}
	defer rdb.Close()

	limiter := ratelimiter.NewRedisSlidingWindow(rdb, b.N*100, time.Hour)
	defer limiter.Close()

	ctx := context.Background()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			limiter.Allow(ctx, "bench-key") //nolint:errcheck
		}
	})
}
