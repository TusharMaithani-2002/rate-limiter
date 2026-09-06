package middleware

import (
	"fmt"
	"log"
	"net/http"
	"rate-limiter/limiter"
	"strconv"
	"time"
)

func RedisRateLimit(rl *limiter.RedisLimiter, keyFunc KeyFunc, maxBurst int) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := keyFunc(r)

			// Use request context so that if the HTTP client disconnects,
			// the Redis query is aborted promptly
			allowed, remainingTokens, retryAfter, err := rl.Allow(r.Context(), key)
			if err != nil {
				// Resiliency: Fail open so Redis downtime does not bring down the entire API
				log.Printf("WARN: Rate limiter failed for key %s: %v. Failing open.", key, err)
				next.ServeHTTP(w, r)
				return
			}

			w.Header().Set("X-RateLimit-Limit", strconv.Itoa(maxBurst))
			w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(int(remainingTokens)))

			if !allowed {
				// selecting 1 second as the minimum retry duration}
				retrySeconds := max(int(retryAfter.Round(time.Second).Seconds()), 1)
				w.Header().Set("Retry-After", strconv.Itoa(retrySeconds))
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusTooManyRequests)
				fmt.Fprintf(w, `{"error": "too many requests", "retry_after_seconds": %d}`, retrySeconds)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
