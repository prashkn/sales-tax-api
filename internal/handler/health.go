package handler

import (
	"math"
	"net/http"
	"time"

	"github.com/prashkn/sales-tax-api/internal/cache"
	"github.com/prashkn/sales-tax-api/internal/service"
	"github.com/prashkn/sales-tax-api/internal/store"
)

type HealthHandler struct {
	store      *store.Store
	cache      *cache.Cache
	taxService *service.TaxService
}

func NewHealthHandler(s *store.Store, c *cache.Cache, ts *service.TaxService) *HealthHandler {
	return &HealthHandler{store: s, cache: c, taxService: ts}
}

func (h *HealthHandler) Health(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	status := http.StatusOK

	dbStatus := "ok"
	if err := h.store.Ping(ctx); err != nil {
		dbStatus = "error: " + err.Error()
		status = http.StatusServiceUnavailable
	}

	redisStatus := "ok"
	if err := h.cache.Ping(ctx); err != nil {
		redisStatus = "error: " + err.Error()
		status = http.StatusServiceUnavailable
	}

	resp := map[string]any{
		"status":   "healthy",
		"database": dbStatus,
		"redis":    redisStatus,
	}

	if status != http.StatusOK {
		resp["status"] = "degraded"
	}

	// Data freshness: report age and version of the latest rate data.
	if df, err := h.taxService.GetDataFreshness(ctx); err == nil {
		ageDays := math.Floor(time.Since(df.LastUpdated).Hours() / 24)
		data := map[string]any{
			"last_updated": df.LastUpdated.Format(time.DateOnly),
			"age_days":     ageDays,
			"record_count": df.RecordCount,
		}
		// Warn if data is older than 100 days (quarterly updates + buffer).
		if ageDays > 100 {
			data["warning"] = "data may be stale, expected quarterly refresh"
		}
		resp["data"] = data
	}

	writeJSON(w, status, resp)
}
