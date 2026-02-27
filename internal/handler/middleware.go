package handler

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/prashkn/sales-tax-api/internal/apikey"
)

type contextKey string

const apiKeyContextKey contextKey = "api_key"

// APIKeyAuth validates the API key from request headers and stores it in
// the request context for downstream middleware (e.g., rate limiter).
func APIKeyAuth(validator *apikey.Validator, rapidAPISecret string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := r.Header.Get("X-API-Key")
			if key == "" {
				if auth := r.Header.Get("Authorization"); strings.HasPrefix(auth, "Bearer ") {
					key = strings.TrimPrefix(auth, "Bearer ")
				}
			}

			// RapidAPI requests: validate the proxy secret header.
			if proxySecret := r.Header.Get("X-RapidAPI-Proxy-Secret"); proxySecret != "" {
				if rapidAPISecret == "" || proxySecret != rapidAPISecret {
					writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid rapidapi proxy secret"})
					return
				}
				// Use the RapidAPI user's subscription key for identity.
				if rapidUser := r.Header.Get("X-RapidAPI-User"); rapidUser != "" {
					key = "rapid:" + rapidUser
				} else {
					key = "rapid:" + proxySecret[:16]
				}
			}

			if key == "" {
				writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "missing api key"})
				return
			}

			if err := validator.Validate(key); err != nil {
				writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid api key"})
				return
			}

			// Store key in context for downstream middleware.
			ctx := context.WithValue(r.Context(), apiKeyContextKey, key)
			next.ServeHTTP(w, r.WithContext(ctx))
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
			// Prefer API key identity set by auth middleware.
			key, _ := r.Context().Value(apiKeyContextKey).(string)
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
