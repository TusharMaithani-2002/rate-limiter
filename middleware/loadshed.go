package middleware

import (
	"fmt"
	"net/http"
	"rate-limiter/shedder"
)

func LoadShed(shedder *shedder.ConcurrencyShedder) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			release, err := shedder.Acquire()
			if err != nil {
				// Server is at full capacity: shed load immediately
				w.Header().Set("Retry-After", "1")
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusServiceUnavailable)
				fmt.Fprintf(w, `{"error": "service unavailable", "message": "server overloaded, shedding load"}`)
				return
			}

			defer release()
			next.ServeHTTP(w, r)
		})
	}
}
