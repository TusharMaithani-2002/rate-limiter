package limiter

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

type HybridLimiter struct {
	redisLimiter       *RedisLimiter
	fallbackMemLimiter *MemoryLimiter
	syncScript         *redis.Script
	rate               float64
	capacity           float64

	// Circuit breaker state: 0 = healthy (Redis), 1 = degraded (Local memory fallback)
	isOffline atomic.Int32
	// Offline tracker: maps clientKey -> number of tokens consumed during outage
	deltaMu     sync.Mutex
	consumedMap map[string]float64
}

func NewHybridLimiter(
	rl *RedisLimiter,
	ml *MemoryLimiter,
	rate float64,
	capacity float64,
) *HybridLimiter {
	return &HybridLimiter{
		redisLimiter:       rl,
		fallbackMemLimiter: ml,
		syncScript:         redis.NewScript(SyncOfflineDeltasLuaScript),
		rate:               rate,
		capacity:           capacity,
		consumedMap:        make(map[string]float64),
	}
}

// Allow attempts Redis first. If Redis fails, it falls back to local memory seamlessly.
func (hl *HybridLimiter) Allow(ctx context.Context, key string) (bool, float64, time.Duration, error) {
	// If Redis is marked offline, route straight to memory to save latency
	if hl.isOffline.Load() == 1 {
		return hl.allowFallback(key)
	}

	allowed, remaining, retryAfter, err := hl.redisLimiter.Allow(ctx, key)

	if err != nil {
		// Trip the breaker to offline mode
		if hl.isOffline.CompareAndSwap(0, 1) {
			log.Printf("ALERT: Redis connection lost (%v). Failing over to In-Memory Limiter.", err)
		}
		return hl.allowFallback(key)
	}

	return allowed, remaining, retryAfter, nil
}

func (hl *HybridLimiter) allowFallback(key string) (bool, float64, time.Duration, error) {
	bucket := hl.fallbackMemLimiter.GetBucket(key)
	allowed, remaining, retryAfter := bucket.Allow()

	if allowed {
		// Record that a token was consumed offline so we can deduct it when Redis recovers
		hl.deltaMu.Lock()
		hl.consumedMap[key] += 1.0
		hl.deltaMu.Unlock()
	}
	return allowed, remaining, retryAfter, nil
}

// StartRecoverySupervisor monitors Redis health and flushes local debits upon reconnect.
func (hl *HybridLimiter) StartRecoverySupervisor(ctx context.Context, rdb *redis.Client, checkInterval time.Duration) {
	ticker := time.NewTicker(checkInterval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if hl.isOffline.Load() == 1 {
					hl.probeAndRecover(ctx, rdb)
				}
			}
		}
	}()
}

func (hl *HybridLimiter) probeAndRecover(ctx context.Context, rdb *redis.Client) {
	pingCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	if err := rdb.Ping(pingCtx).Err(); err != nil {
		// Redis is still down
		return
	}

	log.Println("NOTICE: Redis connection re-established! Synchronizing offline usage...")

	// 1. Drain the offline token debits
	hl.deltaMu.Lock()
	pendingSync := hl.consumedMap
	hl.consumedMap = make(map[string]float64)
	hl.deltaMu.Unlock()

	now := float64(time.Now().UnixNano()) / 1e9

	// 2. Commit debits to Redis via Lua
	for key, delta := range pendingSync {
		redisKey := "ratelimit:" + key
		_, err := hl.syncScript.Run(ctx, rdb, []string{redisKey}, hl.rate, hl.capacity, now, delta).Result()
		if err != nil {
			log.Printf("ERROR: Failed to reconcile offline delta for %s: %v", key, err)
		}
	}

	// 3. Mark system as healthy
	hl.isOffline.Store(0)
	log.Println("SUCCESS: Limiter restored to distributed Redis mode.")
}
