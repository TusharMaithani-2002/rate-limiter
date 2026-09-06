package limiter

import (
	"context"
	"sync"
	"time"
)

type ClientEntry struct {
	bucket   *TokenBucket
	lastSeen time.Time
}

type MemoryLimiter struct {
	mu           sync.RWMutex
	clients      map[string]*ClientEntry
	rate         float64
	burst        float64
	entryTTL     time.Duration
	cleanupEvery time.Duration
	something    func()
}

func NewMemoryLimiter(rate, burst float64, entryTTL, cleanupEvery time.Duration) *MemoryLimiter {
	return &MemoryLimiter{
		clients:      make(map[string]*ClientEntry),
		rate:         rate,
		burst:        burst,
		entryTTL:     entryTTL,
		cleanupEvery: cleanupEvery,
	}
}

func (ml *MemoryLimiter) GetBucket(key string) *TokenBucket {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	now := time.Now()
	entry, exists := ml.clients[key]

	if exists {
		entry.lastSeen = now
		return entry.bucket
	}

	newBucket := NewTokenBucket(ml.rate, ml.burst)
	ml.clients[key] = &ClientEntry{
		bucket:   newBucket,
		lastSeen: now,
	}

	return newBucket
}

func (ml *MemoryLimiter) Cleanup(ctx context.Context) {
	ticker := time.NewTicker(ml.cleanupEvery)

	go func() {
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done(): // Clean exit when server shuts down
				return
			case <-ticker.C:
				ml.evictStaleEntries()
			}
		}
	}()
}

func (ml *MemoryLimiter) evictStaleEntries() {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	now := time.Now()
	for key, entry := range ml.clients {
		if now.Sub(entry.lastSeen) > ml.entryTTL {
			delete(ml.clients, key)
		}
	}
}
