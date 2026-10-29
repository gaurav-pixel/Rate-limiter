package tests

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/yourname/rate-limiter-gateway/ratelimiter"
)

// ---- helpers ----------------------------------------------------------------

// redisClient returns a connected client or nil if Redis is unavailable.
func redisClient() *redis.Client {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		rdb.Close()
		return nil
	}
	return rdb
}

type loadResult struct {
	allowed  int64
	rejected int64
	errors   int64
	duration time.Duration
}

// runLoad fires goroutines * requestsPerGoroutine requests against limiter
// and returns counts of allowed / rejected / error responses.
func runLoad(limiter ratelimiter.RateLimiter, goroutines, requestsPerGoroutine int) loadResult {
	var allowed, rejected, errs int64

	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("user-%d", id)
			for i := 0; i < requestsPerGoroutine; i++ {
				ok, err := limiter.Allow(context.Background(), key)
				if err != nil {
					atomic.AddInt64(&errs, 1)
				} else if ok {
					atomic.AddInt64(&allowed, 1)
				} else {
					atomic.AddInt64(&rejected, 1)
				}
			}
		}(g)
	}

	wg.Wait()
	return loadResult{
		allowed:  atomic.LoadInt64(&allowed),
		rejected: atomic.LoadInt64(&rejected),
		errors:   atomic.LoadInt64(&errs),
		duration: time.Since(start),
	}
}

// ---- in-memory token bucket -------------------------------------------------

func TestInMemoryTokenBucketLoad(t *testing.T) {
	const (
		goroutines           = 100
		requestsPerGoroutine = 50
		capacity             = 10.0
		refillRate           = 1.0
	)

	limiter := ratelimiter.NewTokenBucket(capacity, refillRate)
	defer limiter.Close()

	res := runLoad(limiter, goroutines, requestsPerGoroutine)
	total := goroutines * requestsPerGoroutine

	t.Logf("Token Bucket Load Test")
	t.Logf("  goroutines   : %d", goroutines)
	t.Logf("  requests     : %d", total)
	t.Logf("  allowed      : %d", res.allowed)
	t.Logf("  rejected     : %d", res.rejected)
	t.Logf("  errors       : %d", res.errors)
	t.Logf("  duration     : %s", res.duration)
	t.Logf("  throughput   : %.0f req/s", float64(total)/res.duration.Seconds())

	if res.errors > 0 {
		t.Errorf("unexpected errors: %d", res.errors)
	}
	if res.allowed+res.rejected != int64(total) {
		t.Errorf("allowed(%d) + rejected(%d) != total(%d)", res.allowed, res.rejected, total)
	}
}

func TestInMemoryTokenBucketBurst(t *testing.T) {
	const capacity = 50.0
	limiter := ratelimiter.NewTokenBucket(capacity, 1.0) // slow refill
	defer limiter.Close()

	const total = 1000
	const key = "burst-user"
	var allowed, rejected int64

	var wg sync.WaitGroup
	wg.Add(total)
	for i := 0; i < total; i++ {
		go func() {
			defer wg.Done()
			ok, _ := limiter.Allow(context.Background(), key)
			if ok {
				atomic.AddInt64(&allowed, 1)
			} else {
				atomic.AddInt64(&rejected, 1)
			}
		}()
	}
	wg.Wait()

	t.Logf("Burst Test (capacity=%v, total=%d)", capacity, total)
	t.Logf("  allowed  : %d", allowed)
	t.Logf("  rejected : %d", rejected)

	if allowed > int64(capacity)+5 { // +5 small tolerance for timing
		t.Errorf("allowed %d exceeds capacity %v by too much", allowed, capacity)
	}
	if rejected < int64(total)-int64(capacity)-5 {
		t.Errorf("expected more rejections, got only %d", rejected)
	}
}

// ---- in-memory sliding window -----------------------------------------------

func TestInMemorySlidingWindowLoad(t *testing.T) {
	const (
		goroutines           = 100
		requestsPerGoroutine = 50
		limit                = 10
	)

	limiter := ratelimiter.NewSlidingWindow(limit, time.Minute)
	defer limiter.Close()

	res := runLoad(limiter, goroutines, requestsPerGoroutine)
	total := goroutines * requestsPerGoroutine

	t.Logf("Sliding Window Load Test")
	t.Logf("  goroutines   : %d", goroutines)
	t.Logf("  requests     : %d", total)
	t.Logf("  allowed      : %d", res.allowed)
	t.Logf("  rejected     : %d", res.rejected)
	t.Logf("  errors       : %d", res.errors)
	t.Logf("  duration     : %s", res.duration)
	t.Logf("  throughput   : %.0f req/s", float64(total)/res.duration.Seconds())

	if res.errors > 0 {
		t.Errorf("unexpected errors: %d", res.errors)
	}
	if res.allowed+res.rejected != int64(total) {
		t.Errorf("allowed(%d) + rejected(%d) != total(%d)", res.allowed, res.rejected, total)
	}
}

// ---- Redis token bucket -----------------------------------------------------

func TestRedisTokenBucketLoad(t *testing.T) {
	rdb := redisClient()
	if rdb == nil {
		t.Skip("Redis not available – set REDIS_ADDR or start Redis on localhost:6379")
	}
	defer rdb.Close()

	limiter := ratelimiter.NewRedisTokenBucket(rdb, 10, 1.0)

	const (
		goroutines           = 50
		requestsPerGoroutine = 20
	)
	res := runLoad(limiter, goroutines, requestsPerGoroutine)
	total := goroutines * requestsPerGoroutine

	t.Logf("Redis Token Bucket Load Test")
	t.Logf("  goroutines   : %d", goroutines)
	t.Logf("  requests     : %d", total)
	t.Logf("  allowed      : %d", res.allowed)
	t.Logf("  rejected     : %d", res.rejected)
	t.Logf("  errors       : %d", res.errors)
	t.Logf("  duration     : %s", res.duration)
	t.Logf("  throughput   : %.0f req/s", float64(total)/res.duration.Seconds())

	if res.errors > 0 {
		t.Errorf("unexpected errors: %d", res.errors)
	}
}

// ---- Redis sliding window ---------------------------------------------------

func TestRedisSlidingWindowLoad(t *testing.T) {
	rdb := redisClient()
	if rdb == nil {
		t.Skip("Redis not available – set REDIS_ADDR or start Redis on localhost:6379")
	}
	defer rdb.Close()

	limiter := ratelimiter.NewRedisSlidingWindow(rdb, 10, time.Minute)

	const (
		goroutines           = 50
		requestsPerGoroutine = 20
	)
	res := runLoad(limiter, goroutines, requestsPerGoroutine)
	total := goroutines * requestsPerGoroutine

	t.Logf("Redis Sliding Window Load Test")
	t.Logf("  goroutines   : %d", goroutines)
	t.Logf("  requests     : %d", total)
	t.Logf("  allowed      : %d", res.allowed)
	t.Logf("  rejected     : %d", res.rejected)
	t.Logf("  errors       : %d", res.errors)
	t.Logf("  duration     : %s", res.duration)
	t.Logf("  throughput   : %.0f req/s", float64(total)/res.duration.Seconds())

	if res.errors > 0 {
		t.Errorf("unexpected errors: %d", res.errors)
	}
}

// ---- comparison table -------------------------------------------------------

func TestCompareImplementations(t *testing.T) {
	const (
		goroutines           = 50
		requestsPerGoroutine = 100
		capacity             = 20.0
		limit                = 20
		refillRate           = 5.0
	)

	implementations := []struct {
		name    string
		factory func() ratelimiter.RateLimiter
	}{
		{
			name: "InMemoryTokenBucket",
			factory: func() ratelimiter.RateLimiter {
				return ratelimiter.NewTokenBucket(capacity, refillRate)
			},
		},
		{
			name: "InMemorySlidingWindow",
			factory: func() ratelimiter.RateLimiter {
				return ratelimiter.NewSlidingWindow(limit, time.Minute)
			},
		},
	}

	// Add Redis variants if available.
	rdb := redisClient()
	if rdb != nil {
		defer rdb.Close()
		implementations = append(implementations,
			struct {
				name    string
				factory func() ratelimiter.RateLimiter
			}{
				name: "RedisTokenBucket",
				factory: func() ratelimiter.RateLimiter {
					return ratelimiter.NewRedisTokenBucket(rdb, capacity, refillRate)
				},
			},
			struct {
				name    string
				factory func() ratelimiter.RateLimiter
			}{
				name: "RedisSlidingWindow",
				factory: func() ratelimiter.RateLimiter {
					return ratelimiter.NewRedisSlidingWindow(rdb, limit, time.Minute)
				},
			},
		)
	}

	fmt.Printf("\n%-30s %10s %10s %10s %12s %12s\n",
		"Implementation", "Allowed", "Rejected", "Errors", "Duration", "Req/s")
	fmt.Printf("%s\n", fmt.Sprintf("%0*d", 90, 0))

	for _, impl := range implementations {
		l := impl.factory()
		res := runLoad(l, goroutines, requestsPerGoroutine)
		l.Close()

		total := goroutines * requestsPerGoroutine
		rps := float64(total) / res.duration.Seconds()
		fmt.Printf("%-30s %10d %10d %10d %12s %12.0f\n",
			impl.name, res.allowed, res.rejected, res.errors, res.duration.Round(time.Millisecond), rps)
	}
	fmt.Println()
}
