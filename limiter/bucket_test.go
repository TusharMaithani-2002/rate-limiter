package limiter

import (
	"sync"
	"testing"
	"time"
)

func TestTokenBucket_BurstAndRefill(t *testing.T) {
	// Rate: 10 tokens/sec, Capacity: 5 tokens
	capacity := 5
	tb := NewTokenBucket(10, float64(capacity))

	for i := range capacity {
		allowed, _, _ := tb.Allow()
		if !allowed {
			t.Errorf("Expected request %d to be allowed, but it was denied", i+1)
		}
	}

	// 6th request should be denied
	allowed, _, retryAfter := tb.Allow()
	if allowed {
		t.Fatalf("expected 6th request to be denied")
	}

	if retryAfter <= 0 {
		t.Fatalf("expected positive retry-after duration, got: %v", retryAfter)
	}

	// Wait 200ms -> should replenish ~2 tokens (10 tokens/sec * 0.2s)
	time.Sleep(210 * time.Millisecond)

	allowed, _, _ = tb.Allow()
	if !allowed {
		t.Fatalf("expected request after refill to succeed")
	}
}

func TestTockenBucket_ConcurrencyRate(t *testing.T) {
	tb := NewTokenBucket(100, 50)
	var wg sync.WaitGroup
	workers := 10000

	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			for range 50 {
				tb.Allow()
			}
		}()
	}

	wg.Wait()
}
