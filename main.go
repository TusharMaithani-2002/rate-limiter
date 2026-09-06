package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"rate-limiter/limiter"
	"rate-limiter/middleware"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

func main() {
	fmt.Println("Distributed In-Memory Rate Limiter & Concurrency Limiter (Load Shedder)")

	burst := 10.0
	rate := 5.0

	// Initialize Limiter Manager with auto-cleanup (evicts idle clients after 5m)
	// limiterMgr := limiter.NewMemoryLimiter(rate, burst, 5*time.Minute, 1*time.Minute)

	rdb := redis.NewClient(&redis.Options{
		Addr:         "localhost:6379",
		Password:     "",
		DB:           0,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  500 * time.Millisecond,
		WriteTimeout: 500 * time.Millisecond,
		PoolSize:     100,
	})

	defer rdb.Close()

	// Clean shutdown context for background eviction loop
	// ctx, cancel := context.WithCancel(context.Background())
	// defer cancel()
	// limiterMgr.Cleanup(ctx)

	// pingCtx, pingCancel := context.WithTimeout(context.Background(), 2*time.Second)
	// defer pingCancel()
	// if err := rdb.Ping(pingCtx).Err(); err != nil {
	// 	log.Fatalf("Redis connection failed: %v", err)
	// }

	redisLimiter := limiter.NewRedisLimiter(rdb, rate, burst)
	memLimiter := limiter.NewMemoryLimiter(rate, burst, 5*time.Minute, 1*time.Minute)

	hybridLimiter := limiter.NewHybridLimiter(redisLimiter, memLimiter, rate, burst)

	// Application lifecycle context
	appCtx, cancelApp := context.WithCancel(context.Background())
	defer cancelApp()

	// Start janitors
	memLimiter.Cleanup(appCtx)
	hybridLimiter.StartRecoverySupervisor(appCtx, rdb, 2*time.Second) // Checks Redis every 2s

	mux := http.NewServeMux()
	mux.HandleFunc("/api/data", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status": "success", "message": "Request passed through!"}`))
	})

	// Wrap mux with the RateLimit middleware using client IP as key
	// rateLimitedHandler := middleware.RateLimit(
	// 	limiterMgr,
	// 	middleware.ExtractClientIP, // Extracts the client IP to identify whom to limit
	// 	int(burst),
	// )(mux)

	// redistrLimitedHandler := middleware.RedisRateLimit(
	// 	redisLimiter,
	// 	middleware.ExtractClientIP, // Extracts the client IP to identify whom to limit
	// 	int(burst),
	// )(mux)

	hybridLimiterHandler := middleware.HybridRateLimit(
		hybridLimiter, middleware.ExtractClientIP,
		int(burst),
	)(mux)

	// Configure HTTP Server with production timeouts
	server := &http.Server{
		Addr:         ":8080",
		Handler:      hybridLimiterHandler,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Run server in background goroutine & wait for shutdown signal
	go func() {
		log.Println("Server running on http://localhost:8080")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server listen error: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit // this is breaking statement as SIGINT and SIGNTERM will come out from here

	log.Println("Shutting down server gracefully...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("Forced shutdown error: %v", err)
	}
	log.Println("Server exited cleanly.")
}
