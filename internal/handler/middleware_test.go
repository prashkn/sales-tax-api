package handler

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prashkn/sales-tax-api/internal/apikey"
)

const testSecret = "test-secret-for-middleware"

func okHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func TestAPIKeyAuth_MissingKey(t *testing.T) {
	v := apikey.NewValidator(testSecret)
	mw := APIKeyAuth(v, "")

	req := httptest.NewRequest("GET", "/", nil)
	rr := httptest.NewRecorder()
	mw(http.HandlerFunc(okHandler)).ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr.Code)
	}
}

func TestAPIKeyAuth_ValidDirectKey(t *testing.T) {
	v := apikey.NewValidator(testSecret)
	key := v.GenerateKey("test_customer")

	mw := APIKeyAuth(v, "")

	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("X-API-Key", key)
	rr := httptest.NewRecorder()
	mw(http.HandlerFunc(okHandler)).ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
}

func TestAPIKeyAuth_BearerToken(t *testing.T) {
	v := apikey.NewValidator(testSecret)
	key := v.GenerateKey("bearer_customer")

	mw := APIKeyAuth(v, "")

	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Authorization", "Bearer "+key)
	rr := httptest.NewRecorder()
	mw(http.HandlerFunc(okHandler)).ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
}

func TestAPIKeyAuth_RapidAPI_ValidSecret(t *testing.T) {
	v := apikey.NewValidator(testSecret)
	proxySecret := "rapidapi-proxy-secret-value"

	mw := APIKeyAuth(v, proxySecret)

	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("X-RapidAPI-Proxy-Secret", proxySecret)
	req.Header.Set("X-RapidAPI-User", "someuser")
	rr := httptest.NewRecorder()
	mw(http.HandlerFunc(okHandler)).ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
}

func TestAPIKeyAuth_RapidAPI_InvalidSecret(t *testing.T) {
	v := apikey.NewValidator(testSecret)

	mw := APIKeyAuth(v, "the-real-secret")

	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("X-RapidAPI-Proxy-Secret", "wrong-secret")
	rr := httptest.NewRecorder()
	mw(http.HandlerFunc(okHandler)).ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr.Code)
	}
}

func TestAPIKeyAuth_SetsContextKey(t *testing.T) {
	v := apikey.NewValidator(testSecret)
	key := v.GenerateKey("ctx_customer")

	var gotKey string
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey, _ = r.Context().Value(apiKeyContextKey).(string)
		w.WriteHeader(http.StatusOK)
	})

	mw := APIKeyAuth(v, "")
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("X-API-Key", key)
	rr := httptest.NewRecorder()
	mw(inner).ServeHTTP(rr, req)

	if gotKey != key {
		t.Fatalf("expected context key %q, got %q", key, gotKey)
	}
}

func TestRateLimiter_AllowsBurst(t *testing.T) {
	mw := RateLimiter(5) // 5 rps, burst=10

	handler := mw(http.HandlerFunc(okHandler))

	// First 10 requests should succeed (burst).
	for i := 0; i < 10; i++ {
		req := httptest.NewRequest("GET", "/", nil)
		req.Header.Set("X-API-Key", "test-key")
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("request %d: expected 200, got %d", i+1, rr.Code)
		}
	}

	// 11th request should be rate-limited.
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("X-API-Key", "test-key")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d", rr.Code)
	}
}

func TestRateLimiter_DifferentKeysIndependent(t *testing.T) {
	mw := RateLimiter(1) // 1 rps, burst=2

	handler := mw(http.HandlerFunc(okHandler))

	// Exhaust key A's bucket.
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest("GET", "/", nil)
		req.Header.Set("X-API-Key", "key-a")
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
	}

	// Key B should still have tokens.
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("X-API-Key", "key-b")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 for key-b, got %d", rr.Code)
	}
}

func TestRateLimiter_Refills(t *testing.T) {
	mw := RateLimiter(100) // 100 rps, burst=200

	handler := mw(http.HandlerFunc(okHandler))

	// Exhaust the bucket.
	for i := 0; i < 200; i++ {
		req := httptest.NewRequest("GET", "/", nil)
		req.Header.Set("X-API-Key", "refill-key")
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
	}

	// Wait for refill (at 100/s, 50ms gives ~5 tokens).
	time.Sleep(50 * time.Millisecond)

	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("X-API-Key", "refill-key")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 after refill, got %d", rr.Code)
	}
}
