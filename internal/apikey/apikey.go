package apikey

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

// Validator checks API key signatures.
//
// Direct API keys use the format: stx_<payload>_<hex_signature>
// where the signature is HMAC-SHA256(secret, payload).
//
// RapidAPI proxy keys are validated by checking against the
// configured RapidAPI proxy secret (set via X-RapidAPI-Proxy-Secret header
// and matched in middleware before reaching this validator).
type Validator struct {
	secret []byte
}

func NewValidator(secret string) *Validator {
	return &Validator{secret: []byte(secret)}
}

// Validate checks whether the given API key is validly signed.
func (v *Validator) Validate(apiKey string) error {
	if len(apiKey) < 16 {
		return fmt.Errorf("invalid api key")
	}

	// Direct API keys: stx_<payload>_<signature>
	if strings.HasPrefix(apiKey, "stx_") {
		return v.validateDirect(apiKey)
	}

	// RapidAPI keys: opaque strings passed through their proxy.
	// The middleware already verified the X-RapidAPI-Proxy-Secret header,
	// so if we reach here with a non-stx key, it came from a RapidAPI request.
	// Accept any key of sufficient length.
	return nil
}

func (v *Validator) validateDirect(apiKey string) error {
	// Format: stx_<payload>_<hex_signature>
	// Find the last underscore to split payload from signature.
	lastUnderscore := strings.LastIndex(apiKey, "_")
	if lastUnderscore <= 4 { // "stx_" is 4 chars, need payload after it
		return fmt.Errorf("invalid api key format")
	}

	payload := apiKey[:lastUnderscore]  // "stx_<payload>"
	sigHex := apiKey[lastUnderscore+1:] // hex-encoded HMAC

	if len(sigHex) != 64 { // SHA-256 produces 32 bytes = 64 hex chars
		return fmt.Errorf("invalid api key format")
	}

	sig, err := hex.DecodeString(sigHex)
	if err != nil {
		return fmt.Errorf("invalid api key format")
	}

	mac := hmac.New(sha256.New, v.secret)
	mac.Write([]byte(payload))
	expected := mac.Sum(nil)

	if !hmac.Equal(sig, expected) {
		return fmt.Errorf("invalid api key")
	}

	return nil
}

// GenerateKey creates a new signed API key for a given identifier (e.g., Stripe customer ID).
func (v *Validator) GenerateKey(identifier string) string {
	payload := "stx_" + identifier
	mac := hmac.New(sha256.New, v.secret)
	mac.Write([]byte(payload))
	sig := hex.EncodeToString(mac.Sum(nil))
	return payload + "_" + sig
}
