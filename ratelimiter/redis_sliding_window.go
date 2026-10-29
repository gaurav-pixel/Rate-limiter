package ratelimiter

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// slidingWindowLua is the atomic Lua script for the Redis sliding window.
//
// KEYS[1] – sorted set key for this rate limit key
// ARGV[1] – current time in milliseconds
// ARGV[2] – window size in milliseconds
// ARGV[3] – request limit (int)
// ARGV[4] – number of requests (int)
// ARGV[5] – unique ID prefix to avoid score collisions within the same millisecond
//
// Returns: {allowed (0|1), remaining}
const slidingWindowLua = `
local key       = KEYS[1]
local now       = tonumber(ARGV[1])
local window    = tonumber(ARGV[2])
local limit     = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])
local uid       = ARGV[5]

local window_start = now - window

redis.call('ZREMRANGEBYSCORE', key, '-inf', window_start)
local count = redis.call('ZCARD', key)

if count + requested <= limit then
    for i = 1, requested do
        redis.call('ZADD', key, now, uid .. ':' .. i)
    end
    redis.call('PEXPIRE', key, window)
    return {1, limit - count - requested}
else
    return {0, 0}
end
`

// RedisSlidingWindow is a distributed sliding window rate limiter backed by Redis.
//
// Each request's timestamp is stored in a Redis sorted set (ZSET). An atomic
// Lua script evicts stale entries, checks the current count, and adds new
// entries in a single round-trip – preventing race conditions across multiple
// gateway instances.
type RedisSlidingWindow struct {
	client    *redis.Client
	limit     int
	window    time.Duration
	script    *redis.Script
	keyPrefix string
}

// NewRedisSlidingWindow creates a new Redis-backed sliding window limiter.
//
//   - client – connected Redis client
//   - limit  – maximum requests allowed within window
//   - window – rolling time window size
func NewRedisSlidingWindow(client *redis.Client, limit int, window time.Duration) *RedisSlidingWindow {
	return &RedisSlidingWindow{
		client:    client,
		limit:     limit,
		window:    window,
		script:    redis.NewScript(slidingWindowLua),
		keyPrefix: "rl:sw:",
	}
}

// Allow checks whether a single request for key is permitted.
func (r *RedisSlidingWindow) Allow(ctx context.Context, key string) (bool, error) {
	return r.AllowN(ctx, key, 1)
}

// AllowN checks whether n requests for key are permitted atomically.
func (r *RedisSlidingWindow) AllowN(ctx context.Context, key string, n int) (bool, error) {
	if n <= 0 {
		return false, fmt.Errorf("n must be positive, got %d", n)
	}

	now := time.Now().UnixMilli()
	windowMs := r.window.Milliseconds()
	uid := uuid.New().String()

	result, err := r.script.Run(ctx, r.client,
		[]string{r.keyPrefix + key},
		now,
		windowMs,
		r.limit,
		n,
		uid,
	).Slice()
	if err != nil {
		return false, fmt.Errorf("redis sliding window script: %w", err)
	}

	if len(result) < 2 {
		return false, fmt.Errorf("unexpected script result length: %d", len(result))
	}

	allowed, ok := result[0].(int64)
	if !ok {
		return false, fmt.Errorf("unexpected type for allowed flag: %T", result[0])
	}

	return allowed == 1, nil
}

// Reset removes all entries from the sorted set for key.
func (r *RedisSlidingWindow) Reset(ctx context.Context, key string) error {
	return r.client.Del(ctx, r.keyPrefix+key).Err()
}

// Close closes the underlying Redis client.
func (r *RedisSlidingWindow) Close() error {
	return r.client.Close()
}
