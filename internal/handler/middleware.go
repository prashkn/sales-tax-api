package handler

import (
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/prashkn/sales-tax-api/internal/apikey"
)

func APIKeyAuth(validator *apikey.Validator) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := r.Header.Get("X-API-Key")
			if key == "" {
				key = strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			}
			// Also accept RapidAPI proxy header.
			if rapidKey := r.Header.Get("X-RapidAPI-Proxy-Secret"); rapidKey != "" {
				key = rapidKey
			}

			if key == "" {
				writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "missing api key"})
				return
			}

			if err := validator.Validate(key); err != nil {
				writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid api key"})
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// RateLimiter returns middleware that enforces per-key request rate limits
// using a token bucket algorithm. Each unique API key gets its own bucket
// that refills at rps tokens per second, up to a burst of rps*2.
func RateLimiter(rps int) func(http.Handler) http.Handler {
	var (
		mu      sync.Mutex
		buckets = make(map[string]*bucket)
	)
	burst := rps * 2
	refillRate := float64(rps)

	// Periodically evict stale buckets to prevent memory growth.
	go func() {
		for range time.Tick(5 * time.Minute) {
			mu.Lock()
			cutoff := time.Now().Add(-10 * time.Minute)
			for k, b := range buckets {
				if b.lastAccess.Before(cutoff) {
					delete(buckets, k)
				}
			}
			mu.Unlock()
		}
	}()

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := r.Header.Get("X-API-Key")
			if key == "" {
				key = r.RemoteAddr
			}

			mu.Lock()
			b, ok := buckets[key]
			if !ok {
				b = &bucket{tokens: float64(burst), lastRefill: time.Now()}
				buckets[key] = b
			}
			allowed := b.take(refillRate, float64(burst))
			mu.Unlock()

			if !allowed {
				w.Header().Set("Retry-After", "1")
				writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "rate limit exceeded"})
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

type bucket struct {
	tokens     float64
	lastRefill time.Time
	lastAccess time.Time
}

func (b *bucket) take(refillRate, max float64) bool {
	now := time.Now()
	b.lastAccess = now

	elapsed := now.Sub(b.lastRefill).Seconds()
	b.tokens += elapsed * refillRate
	if b.tokens > max {
		b.tokens = max
	}
	b.lastRefill = now

	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

func RequestLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		slog.Info("request",
			"method", r.Method,
			"path", r.URL.Path,
			"duration", time.Since(start),
			"remote", r.RemoteAddr,
		)
	})
}
