package limiter

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisLimiter manages distributed token buckets backed by Redis.
type RedisLimiter struct {
	client   *redis.Client
	script   *redis.Script
	capacity float64
	rate     float64
}

func NewRedisLimiter(client *redis.Client, rate, capacity float64) *RedisLimiter {
	return &RedisLimiter{
		client:   client,
		script:   redis.NewScript(TokenLuaScript),
		capacity: capacity,
		rate:     rate,
	}
}

// Allow evaluates a request using the client's context and key.
func (rl *RedisLimiter) Allow(ctx context.Context, key string) (bool, float64, time.Duration, error) {
	redisKey := fmt.Sprintf("ratelimit:%s", key)
	now := float64(time.Now().UnixNano()) / 1e9 // Unix time as fractional seconds

	// Execute atomic script via EVALSHA / EVAL
	res, err := rl.script.Run(ctx, rl.client, []string{redisKey}, rl.rate, rl.capacity, now, 1.0).Result()

	if err != nil {
		return false, 0, 0, fmt.Errorf("redis limiter error: %w", err)
	}

	values, ok := res.([]any)
	if !ok || len(values) != 3 {
		return false, 0, 0, fmt.Errorf("unexpected script response format: %v", res)
	}

	allowedInt := values[0].(int64)
	remainingStr := values[1].(string)
	retryAfterStr := values[2].(string)

	remaining, _ := strconv.ParseFloat(remainingStr, 64)
	retryAfterSec, _ := strconv.ParseFloat(retryAfterStr, 64)

	return allowedInt == 1, remaining, time.Duration(retryAfterSec * float64(time.Second)), nil
}
