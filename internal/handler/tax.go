package handler

import (
	"encoding/json"
	"net/http"
	"regexp"

	"github.com/go-chi/chi/v5"
	"github.com/prashkn/sales-tax-api/internal/service"
)

var zipRegex = regexp.MustCompile(`^\d{5}$`)

type TaxHandler struct {
	svc *service.TaxService
}

func NewTaxHandler(svc *service.TaxService) *TaxHandler {
	return &TaxHandler{svc: svc}
}

// GET /v1/tax/zip/{zip_code}
func (h *TaxHandler) LookupByZIP(w http.ResponseWriter, r *http.Request) {
	zip := chi.URLParam(r, "zip_code")
	if !zipRegex.MatchString(zip) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid zip code, must be 5 digits"})
		return
	}

	resp, err := h.svc.LookupByZIP(r.Context(), zip)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, resp)
}

// GET /v1/tax/address?street=...&city=...&state=...&zip=...
func (h *TaxHandler) LookupByAddress(w http.ResponseWriter, r *http.Request) {
	street := r.URL.Query().Get("street")
	city := r.URL.Query().Get("city")
	state := r.URL.Query().Get("state")
	zip := r.URL.Query().Get("zip")

	if zip == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "zip query parameter is required"})
		return
	}

	resp, err := h.svc.LookupByAddress(r.Context(), street, city, state, zip)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, resp)
}

// POST /v1/tax/calculate
func (h *TaxHandler) Calculate(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ZIPCode string  `json:"zip_code"`
		Amount  float64 `json:"amount"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return
	}

	if !zipRegex.MatchString(req.ZIPCode) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid zip code"})
		return
	}
	if req.Amount <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "amount must be positive"})
		return
	}

	resp, err := h.svc.Calculate(r.Context(), req.ZIPCode, req.Amount)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, resp)
}

// POST /v1/tax/bulk
func (h *TaxHandler) Bulk(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ZIPCodes []string `json:"zip_codes"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return
	}

	if len(req.ZIPCodes) == 0 || len(req.ZIPCodes) > 100 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "zip_codes must contain 1-100 entries"})
		return
	}

	results := make(map[string]any, len(req.ZIPCodes))
	for _, zip := range req.ZIPCodes {
		if !zipRegex.MatchString(zip) {
			results[zip] = map[string]string{"error": "invalid zip code"}
			continue
		}
		resp, err := h.svc.LookupByZIP(r.Context(), zip)
		if err != nil {
			results[zip] = map[string]string{"error": err.Error()}
			continue
		}
		results[zip] = resp
	}

	writeJSON(w, http.StatusOK, map[string]any{"results": results})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}