package ratelimiter

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// windowState holds per-key state for the sliding window algorithm.
type windowState struct {
	// timestamps is a sorted (ascending) list of request times within the window.
	timestamps []time.Time
}

// SlidingWindow implements an in-memory sliding window rate limiter.
//
// Each request's timestamp is recorded. Before each check, timestamps older
// than window are evicted. A request is allowed only when the remaining count
// after eviction is less than limit.
type SlidingWindow struct {
	limit   int
	window  time.Duration
	windows map[string]*windowState
	mu      sync.Mutex
}

// NewSlidingWindow creates a new in-memory sliding window limiter.
//
//   - limit  – maximum requests allowed within window
//   - window – rolling time window size
func NewSlidingWindow(limit int, window time.Duration) *SlidingWindow {
	return &SlidingWindow{
		limit:   limit,
		window:  window,
		windows: make(map[string]*windowState),
	}
}

// evict removes timestamps that are outside the current window.
// Must be called with sw.mu held.
func (sw *SlidingWindow) evict(ws *windowState, now time.Time) {
	cutoff := now.Add(-sw.window)
	// Binary search for the first timestamp >= cutoff
	lo, hi := 0, len(ws.timestamps)
	for lo < hi {
		mid := (lo + hi) / 2
		if ws.timestamps[mid].Before(cutoff) {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	ws.timestamps = ws.timestamps[lo:]
}

// getWindow returns the windowState for key, creating it if necessary.
// Must be called with sw.mu held.
func (sw *SlidingWindow) getWindow(key string) *windowState {
	ws, ok := sw.windows[key]
	if !ok {
		ws = &windowState{}
		sw.windows[key] = ws
	}
	return ws
}

// Allow checks whether a single request for key is permitted.
func (sw *SlidingWindow) Allow(_ context.Context, key string) (bool, error) {
	return sw.AllowN(context.Background(), key, 1)
}

// AllowN checks whether n requests for key are permitted atomically.
func (sw *SlidingWindow) AllowN(_ context.Context, key string, n int) (bool, error) {
	if n <= 0 {
		return false, fmt.Errorf("n must be positive, got %d", n)
	}

	sw.mu.Lock()
	defer sw.mu.Unlock()

	now := time.Now()
	ws := sw.getWindow(key)
	sw.evict(ws, now)

	if len(ws.timestamps)+n <= sw.limit {
		for i := 0; i < n; i++ {
			ws.timestamps = append(ws.timestamps, now)
		}
		return true, nil
	}
	return false, nil
}

// Reset clears the sliding window state for the given key.
func (sw *SlidingWindow) Reset(_ context.Context, key string) error {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	delete(sw.windows, key)
	return nil
}

// Close is a no-op for the in-memory implementation.
func (sw *SlidingWindow) Close() error {
	return nil
}

// Remaining returns how many more requests key can make without consuming any quota.
func (sw *SlidingWindow) Remaining(key string) int {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	ws, ok := sw.windows[key]
	if !ok {
		return sw.limit
	}
	sw.evict(ws, time.Now())
	remaining := sw.limit - len(ws.timestamps)
	if remaining < 0 {
		remaining = 0
	}
	return remaining
}
