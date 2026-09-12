package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"rate-limiter/limiter"
	"rate-limiter/middleware"
	"rate-limiter/shedder"
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

	// 2. Initialize Load Shedder (Host protection: max 100 concurrent inflight requests)
	maxConcurrentRequests := 100
	loadShedder := shedder.NewConcurrencyShedder(maxConcurrentRequests)

	mux := http.NewServeMux()
	mux.HandleFunc("/api/data", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(50 * time.Millisecond) // Simulating DB/computation
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status": "success", "message": "Request passed through!"}`))
	})

	// Health & Stats route (Inspect shedder & limiter status live)
	mux.HandleFunc("/api/stats", func(w http.ResponseWriter, r *http.Request) {
		stats := loadShedder.Stats()
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"max_concurrent": %d, "in_flight": %d, "total_shed": %d}`,
			stats.MaxCapacity, stats.InFlight, stats.TotalShed)
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

	pipeline := middleware.LoadShed(loadShedder)(hybridLimiterHandler)

	// Configure HTTP Server with production timeouts
	server := &http.Server{
		Addr:         ":8080",
		Handler:      pipeline,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Testing
	// Register pprof endpoints directly on mux (bypassing limits if called directly or on an admin port)
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

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
