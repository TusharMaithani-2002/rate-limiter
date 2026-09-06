package middleware

import (
	"fmt"
	"net/http"
	"rate-limiter/limiter"
	"strconv"
	"time"
)

// KeyFunc extracts a rate-limiting key from an incoming request.
type KeyFunc func(r *http.Request) string

func RateLimit(manager *limiter.MemoryLimiter, keyFunc KeyFunc, maxBurst int) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Identify the caller (IP, User ID, or API Key)
			key := keyFunc(r)

			// Fetch or initialize the bucket for this specific client
			bucket := manager.GetBucket(key)

			allowed, remainingTokens, retryAfter := bucket.Allow()

			// Set standard RFC rate-limiting headers on all responses
			w.Header().Set("X-RateLimit-Limit", strconv.Itoa(maxBurst))
			w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(int(remainingTokens)))

			if !allowed {
				retrySeconds := max(int(retryAfter.Round(time.Second).Seconds()), 1) // selecting 1 second as the minimum retry duration

				w.Header().Set("Retry-After", strconv.Itoa(retrySeconds))
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusTooManyRequests)

				fmt.Fprintf(w, `{"error": "too many requests", "retry_after_seconds": %d}`, retrySeconds)
				return // Short-circuit: do not invoke the actual handler
			}

			next.ServeHTTP(w, r) // Proceed to the actual handler if allowed
		})
	}
}
