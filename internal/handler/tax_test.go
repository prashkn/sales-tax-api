package handler

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
)

// These tests verify input validation only — they don't need a real service.

func TestLookupByZIP_InvalidZIP(t *testing.T) {
	h := &TaxHandler{svc: nil} // service is nil; we expect validation to reject before calling it.

	tests := []struct {
		zip  string
		code int
	}{
		{"1234", http.StatusBadRequest},     // too short
		{"123456", http.StatusBadRequest},   // too long
		{"abcde", http.StatusBadRequest},    // letters
		{"1234%20", http.StatusBadRequest},  // space (URL-encoded)
	}

	for _, tt := range tests {
		t.Run(tt.zip, func(t *testing.T) {
			r := chi.NewRouter()
			r.Get("/v1/tax/zip/{zip_code}", h.LookupByZIP)

			req := httptest.NewRequest("GET", "/v1/tax/zip/"+tt.zip, nil)
			rr := httptest.NewRecorder()
			r.ServeHTTP(rr, req)

			if rr.Code != tt.code {
				t.Errorf("zip=%q: expected %d, got %d", tt.zip, tt.code, rr.Code)
			}
		})
	}
}

func TestCalculate_InvalidBody(t *testing.T) {
	h := &TaxHandler{svc: nil}

	tests := []struct {
		name string
		body string
		code int
	}{
		{"empty body", "", http.StatusBadRequest},
		{"bad json", "{bad}", http.StatusBadRequest},
		{"bad zip", `{"zip_code":"abc","amount":10}`, http.StatusBadRequest},
		{"zero amount", `{"zip_code":"90210","amount":0}`, http.StatusBadRequest},
		{"negative amount", `{"zip_code":"90210","amount":-5}`, http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/v1/tax/calculate", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/json")
			rr := httptest.NewRecorder()
			h.Calculate(rr, req)

			if rr.Code != tt.code {
				t.Errorf("%s: expected %d, got %d, body: %s", tt.name, tt.code, rr.Code, rr.Body.String())
			}
		})
	}
}

func TestBulk_InvalidBody(t *testing.T) {
	h := &TaxHandler{svc: nil}

	tests := []struct {
		name string
		body string
		code int
	}{
		{"empty body", "", http.StatusBadRequest},
		{"empty array", `{"zip_codes":[]}`, http.StatusBadRequest},
		{"too many", `{"zip_codes":["` + strings.Join(make([]string, 101), `","`) + `"]}`, http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/v1/tax/bulk", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/json")
			rr := httptest.NewRecorder()
			h.Bulk(rr, req)

			if rr.Code != tt.code {
				t.Errorf("%s: expected %d, got %d", tt.name, tt.code, rr.Code)
			}
		})
	}
}

func TestLookupByAddress_MissingZIP(t *testing.T) {
	h := &TaxHandler{svc: nil}

	req := httptest.NewRequest("GET", "/v1/tax/address?street=123+Main&city=LA&state=CA", nil)
	rr := httptest.NewRecorder()
	h.LookupByAddress(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing zip, got %d", rr.Code)
	}
}
