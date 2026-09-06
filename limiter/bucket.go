package limiter

import (
	"sync"
	"time"
)

type TokenBucket struct {
	mu         sync.Mutex
	capacity   float64   // max burst capacity
	rate       float64   // token added per second
	tokens     float64   // current tokens
	lastRefill time.Time // last time tokens were lazily calculated
}

// initialize a new token bucket with capacity and rate
func NewTokenBucket(rate, capacity float64) *TokenBucket {
	return &TokenBucket{
		capacity:   capacity,
		rate:       rate,
		tokens:     capacity,
		lastRefill: time.Now(),
	}
}

func (tb *TokenBucket) Allow() (bool, float64, time.Duration) {
	return tb.AllowN(time.Now(), 1)
}

func (tb *TokenBucket) AllowN(now time.Time, n float64) (bool, float64, time.Duration) {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	elapsed := now.Sub(tb.lastRefill).Seconds()
	tb.lastRefill = now

	tb.tokens = tb.tokens + (elapsed * tb.rate)
	if tb.tokens > tb.capacity {
		tb.tokens = tb.capacity
	}

	// 3. Evaluate if sufficient tokens exist
	if tb.tokens >= n {
		tb.tokens -= n
		return true, tb.tokens, 0
	}

	missingTokens := n - tb.tokens
	retryAfter := time.Duration((missingTokens / tb.rate) * float64(time.Second))
	return false, missingTokens, retryAfter
}
