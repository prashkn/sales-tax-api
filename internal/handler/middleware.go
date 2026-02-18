package handler

import (
	"log/slog"
	"net/http"
	"strings"
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