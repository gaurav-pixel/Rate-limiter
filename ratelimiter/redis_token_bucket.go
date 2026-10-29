package ratelimiter

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// tokenBucketLua is the atomic Lua script for the Redis token bucket.
//
// KEYS[1] – the per-key hash in Redis
// ARGV[1] – capacity (float)
// ARGV[2] – refill rate tokens/sec (float)
// ARGV[3] – requested tokens (int)
// ARGV[4] – current time in milliseconds (int)
//
// Returns: {allowed (0|1), floor(remaining_tokens)}
const tokenBucketLua = `
local key          = KEYS[1]
local capacity     = tonumber(ARGV[1])
local refill_rate  = tonumber(ARGV[2])
local requested    = tonumber(ARGV[3])
local now          = tonumber(ARGV[4])

local bucket      = redis.call('HMGET', key, 'tokens', 'last_refill')
local tokens      = tonumber(bucket[1]) or capacity
local last_refill = tonumber(bucket[2]) or now

local elapsed   = (now - last_refill) / 1000.0
local new_tokens = math.min(capacity, tokens + elapsed * refill_rate)

if new_tokens >= requested then
    new_tokens = new_tokens - requested
    redis.call('HMSET', key, 'tokens', new_tokens, 'last_refill', now)
    redis.call('PEXPIRE', key, math.ceil(capacity / refill_rate * 1000))
    return {1, math.floor(new_tokens)}
else
    redis.call('HMSET', key, 'tokens', new_tokens, 'last_refill', now)
    redis.call('PEXPIRE', key, math.ceil(capacity / refill_rate * 1000))
    return {0, math.floor(new_tokens)}
end
`

// RedisTokenBucket is a distributed token bucket backed by Redis.
//
// All state lives in a Redis hash, and each check/consume cycle is executed
// as a single atomic Lua script so there are no race conditions across
// multiple gateway instances.
type RedisTokenBucket struct {
	client     *redis.Client
	capacity   float64
	refillRate float64 // tokens per second
	script     *redis.Script
	keyPrefix  string
}

// NewRedisTokenBucket creates a new Redis-backed token bucket limiter.
//
//   - client     – connected Redis client
//   - capacity   – maximum tokens (also burst ceiling)
//   - refillRate – tokens added per second
func NewRedisTokenBucket(client *redis.Client, capacity float64, refillRate float64) *RedisTokenBucket {
	return &RedisTokenBucket{
		client:     client,
		capacity:   capacity,
		refillRate: refillRate,
		script:     redis.NewScript(tokenBucketLua),
		keyPrefix:  "rl:tb:",
	}
}

// Allow checks whether a single request for key is permitted.
func (r *RedisTokenBucket) Allow(ctx context.Context, key string) (bool, error) {
	return r.AllowN(ctx, key, 1)
}

// AllowN checks whether n requests for key are permitted atomically.
func (r *RedisTokenBucket) AllowN(ctx context.Context, key string, n int) (bool, error) {
	if n <= 0 {
		return false, fmt.Errorf("n must be positive, got %d", n)
	}

	now := time.Now().UnixMilli()
	result, err := r.script.Run(ctx, r.client,
		[]string{r.keyPrefix + key},
		r.capacity,
		r.refillRate,
		n,
		now,
	).Slice()
	if err != nil {
		return false, fmt.Errorf("redis token bucket script: %w", err)
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

// Reset deletes the Redis hash for the given key, effectively refilling the bucket.
func (r *RedisTokenBucket) Reset(ctx context.Context, key string) error {
	return r.client.Del(ctx, r.keyPrefix+key).Err()
}

// Close closes the underlying Redis client.
func (r *RedisTokenBucket) Close() error {
	return r.client.Close()
}
