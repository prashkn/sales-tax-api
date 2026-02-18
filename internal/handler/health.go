package handler

import (
	"net/http"

	"github.com/prashkn/sales-tax-api/internal/cache"
	"github.com/prashkn/sales-tax-api/internal/store"
)

type HealthHandler struct {
	store *store.Store
	cache *cache.Cache
}

func NewHealthHandler(s *store.Store, c *cache.Cache) *HealthHandler {
	return &HealthHandler{store: s, cache: c}
}

func (h *HealthHandler) Health(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	dbOK := "ok"
	if err := h.store.Ping(ctx); err != nil {
		dbOK = "error: " + err.Error()
	}

	redisOK := "ok"
	if err := h.cache.Ping(ctx); err != nil {
		redisOK = "error: " + err.Error()
	}

	status := http.StatusOK
	if dbOK != "ok" || redisOK != "ok" {
		status = http.StatusServiceUnavailable
	}

	writeJSON(w, status, map[string]any{
		"status":   "healthy",
		"database": dbOK,
		"redis":    redisOK,
	})
}