package middleware

import (
	"fmt"
	"net/http"
	"rate-limiter/limiter"
	"strconv"
	"time"
)

func HybridRateLimit(hl *limiter.HybridLimiter, keyFunc KeyFunc, maxBurst int) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := keyFunc(r)

			allowed, remaining, retryAfter, _ := hl.Allow(r.Context(), key)

			w.Header().Set("X-RateLimit-Limit", strconv.Itoa(maxBurst))
			w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(int(remaining)))

			if !allowed {
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
