# Rate Limiter & API Gateway

A production-grade HTTP API Gateway with pluggable rate limiting written in Go.
Supports both **in-memory** (single-instance) and **Redis-backed** (distributed)
implementations of two classic algorithms: **Token Bucket** and **Sliding Window**.

---

## Architecture

```
                        ┌─────────────────────────────────┐
                        │           main.go               │
                        │  - parse flags                  │
                        │  - build limiter                │
                        │  - wire gateway & server        │
                        └───────────────┬─────────────────┘
                                        │
                        ┌───────────────▼─────────────────┐
                        │          server.Server          │
                        │  - http.Server wrapper          │
                        │  - graceful shutdown            │
                        └───────────────┬─────────────────┘
                                        │
                        ┌───────────────▼─────────────────┐
                        │         gateway.Gateway         │
                        │  - request routing (ServeMux)   │
                        │  - global middleware chain      │
                        └──────┬──────────────────────────┘
                               │ per-route
              ┌────────────────▼──────────────────┐
              │      gateway.RateLimitMiddleware   │
              │  - extract key (IP / header / path)│
              │  - call RateLimiter.Allow()        │
              │  - 429 + Retry-After on rejection  │
              └────────────┬──────────────────────┘
                           │
         ┌─────────────────┴──────────────────┐
         │         ratelimiter.RateLimiter     │
         │              (interface)            │
         └──┬──────────┬────────┬─────────────┘
            │          │        │
    ┌───────┴──┐  ┌────┴───┐  ┌┴──────────────────┐
    │ Token    │  │Sliding │  │ Redis variants     │
    │ Bucket   │  │Window  │  │ (same algorithms,  │
    │(in-mem)  │  │(in-mem)│  │  atomic Lua)       │
    └──────────┘  └────────┘  └────────────────────┘
```

---

## Algorithms

### Token Bucket

Each client gets a "bucket" that holds up to `capacity` tokens. Tokens are
consumed on each request and refilled at `refillRate` per second. Short bursts
are allowed up to the bucket capacity, then requests are throttled.

```
tokens(t) = min(capacity, tokens(t-Δ) + refillRate × Δ)
allowed   = tokens(t) >= requested
```

### Sliding Window

A sorted list of timestamps is kept for each client. Before every check,
timestamps older than `window` are evicted. A request is allowed only when the
number of remaining timestamps is less than `limit`.

```
window_count = |{t ∈ requests : now - t < window}|
allowed      = window_count + requested <= limit
```

### Redis Distributed Coordination

Both Redis implementations execute their state transitions as a single atomic
Lua script per request. This guarantees correctness across multiple gateway
instances without needing distributed locks.

- **Redis Token Bucket** – stores `{tokens, last_refill}` in a Redis hash;
  TTL is set to `capacity / refillRate` seconds.
- **Redis Sliding Window** – stores request timestamps in a Redis sorted set
  (ZSET). Stale entries are pruned with `ZREMRANGEBYSCORE` inside the script;
  TTL equals the window duration.

---

## Quick Start

### Prerequisites

- Go 1.21+
- (optional) Redis 6+

### Build & Run

```bash
cd rate-limiter-gateway

# Download dependencies
go mod tidy

# Run with in-memory token bucket (default)
go run . 

# Run with in-memory sliding window
go run . -algorithm sliding_window -limit 60 -window 1m

# Run with Redis token bucket
go run . -redis-addr localhost:6379 -algorithm token_bucket -burst 100 -refill-rate 10

# Run with Redis sliding window
go run . -redis-addr localhost:6379 -algorithm sliding_window -limit 60 -window 1m
```

### Available Flags

| Flag              | Default        | Description                                      |
|-------------------|----------------|--------------------------------------------------|
| `-redis-addr`     | *(empty)*      | Redis host:port. Empty = in-memory.              |
| `-redis-password` | *(empty)*      | Redis password (optional)                        |
| `-algorithm`      | `token_bucket` | `token_bucket` or `sliding_window`               |
| `-limit`          | `100`          | Max requests per window (sliding window)         |
| `-window`         | `1m`           | Rolling time window (sliding window)             |
| `-burst`          | `20`           | Bucket capacity / burst ceiling (token bucket)   |
| `-refill-rate`    | `10`           | Tokens refilled per second (token bucket)        |
| `-port`           | `8080`         | HTTP listen port                                 |

---

## Endpoints

| Method | Path            | Description                              |
|--------|-----------------|------------------------------------------|
| GET    | `/health`       | Health check – no rate limit             |
| GET    | `/api/v1/hello` | Greeting (`?name=` optional)             |
| POST   | `/api/v1/echo`  | Echoes the request body back             |

### Example requests

```bash
# Health check
curl http://localhost:8080/health

# Hello
curl "http://localhost:8080/api/v1/hello?name=Alice"

# Echo
curl -X POST http://localhost:8080/api/v1/echo \
     -H 'Content-Type: application/json' \
     -d '{"ping":"pong"}'
```

### Rate limit response headers

When a request is allowed the handler response is returned as-is.
When the limit is exceeded the gateway responds:

```
HTTP/1.1 429 Too Many Requests
Content-Type: application/json
Retry-After: 1
X-RateLimit-Remaining: 0
X-RateLimit-Reset: <unix-timestamp>

{"error":"rate limit exceeded","retry_after":1}
```

---

## Testing

```bash
# Run all unit / load tests
go test ./tests/ -v -timeout 60s

# Run only in-memory tests (no Redis required)
go test ./tests/ -v -run "InMemory"

# Run with Redis (set address via env var)
REDIS_ADDR=localhost:6379 go test ./tests/ -v

# Benchmarks (in-memory only)
go test ./tests/ -bench=. -benchmem -run=^$

# Benchmarks including Redis
REDIS_ADDR=localhost:6379 go test ./tests/ -bench=. -benchmem -run=^$
```

---

## Project Structure

```
rate-limiter-gateway/
├── main.go                          # Entry point & flag parsing
├── go.mod
├── config/
│   └── config.go                    # Configuration structs
├── ratelimiter/
│   ├── limiter.go                   # RateLimiter interface & Result
│   ├── token_bucket.go              # In-memory token bucket
│   ├── sliding_window.go            # In-memory sliding window
│   ├── redis_token_bucket.go        # Redis token bucket (Lua)
│   └── redis_sliding_window.go      # Redis sliding window (Lua + ZSET)
├── gateway/
│   ├── gateway.go                   # Router, middleware chain, built-in routes
│   └── middleware.go                # Rate limit middleware, key extractors
├── server/
│   └── server.go                    # HTTP server with graceful shutdown
└── tests/
    ├── load_test.go                 # Concurrent load tests
    └── benchmark_test.go            # Go benchmarks
```

---

## Configuration Reference

`config.RateLimitConfig`

| Field        | Type            | Description                                |
|--------------|-----------------|--------------------------------------------|
| `Algorithm`  | `string`        | `token_bucket` or `sliding_window`         |
| `Limit`      | `int`           | Request quota (used by sliding window)     |
| `Window`     | `time.Duration` | Rolling window size                        |
| `BurstSize`  | `int`           | Burst capacity (token bucket)              |
| `RefillRate` | `float64`       | Tokens per second (token bucket)           |

`config.RedisConfig`

| Field        | Type     | Description                        |
|--------------|----------|------------------------------------|
| `Addr`       | `string` | Redis address (`host:port`)        |
| `Password`   | `string` | Redis AUTH password                |
| `DB`         | `int`    | Redis database index               |
| `PoolSize`   | `int`    | Connection pool size               |
| `MaxRetries` | `int`    | Max retries on transient errors    |
