package apikey

import (
	"strings"
	"testing"
)

const testSecret = "test-secret-key-for-hmac-signing"

func TestGenerateAndValidate(t *testing.T) {
	v := NewValidator(testSecret)

	key := v.GenerateKey("cust_abc123")
	if !strings.HasPrefix(key, "stx_cust_abc123_") {
		t.Fatalf("expected key to start with stx_cust_abc123_, got %s", key)
	}

	if err := v.Validate(key); err != nil {
		t.Fatalf("expected generated key to validate, got error: %v", err)
	}
}

func TestValidate_InvalidSignature(t *testing.T) {
	v := NewValidator(testSecret)

	key := v.GenerateKey("cust_abc123")
	// Corrupt the last character of the signature.
	corrupted := key[:len(key)-1] + "0"
	if key[len(key)-1] == '0' {
		corrupted = key[:len(key)-1] + "1"
	}

	err := v.Validate(corrupted)
	if err == nil {
		t.Fatal("expected validation to fail for corrupted key")
	}
}

func TestValidate_WrongSecret(t *testing.T) {
	v1 := NewValidator("secret-one")
	v2 := NewValidator("secret-two")

	key := v1.GenerateKey("cust_xyz")
	if err := v2.Validate(key); err == nil {
		t.Fatal("expected key signed with different secret to fail")
	}
}

func TestValidate_TooShort(t *testing.T) {
	v := NewValidator(testSecret)
	if err := v.Validate("short"); err == nil {
		t.Fatal("expected short key to fail validation")
	}
}

func TestValidate_BadFormat(t *testing.T) {
	v := NewValidator(testSecret)

	tests := []struct {
		name string
		key  string
	}{
		{"no underscores", "stx_nounderscorehex"},
		{"bad hex", "stx_payload_notvalidhexnotvalidhexnotvalidhexnotvalidhexnotvalidhe"},
		{"empty payload", "stx__" + strings.Repeat("ab", 32)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := v.Validate(tt.key); err == nil {
				t.Errorf("expected key %q to fail validation", tt.key)
			}
		})
	}
}

func TestValidate_RapidAPIKey(t *testing.T) {
	v := NewValidator(testSecret)

	// Non-stx keys >= 16 chars should pass (RapidAPI proxy keys).
	rapidKey := "rapid_1234567890abcdef"
	if err := v.Validate(rapidKey); err != nil {
		t.Fatalf("expected RapidAPI-style key to pass, got: %v", err)
	}
}

func TestGenerateKey_Deterministic(t *testing.T) {
	v := NewValidator(testSecret)
	k1 := v.GenerateKey("cust_001")
	k2 := v.GenerateKey("cust_001")
	if k1 != k2 {
		t.Fatalf("expected deterministic keys, got %s and %s", k1, k2)
	}
}

func TestGenerateKey_UniquePerIdentifier(t *testing.T) {
	v := NewValidator(testSecret)
	k1 := v.GenerateKey("cust_001")
	k2 := v.GenerateKey("cust_002")
	if k1 == k2 {
		t.Fatal("expected different keys for different identifiers")
	}
}
