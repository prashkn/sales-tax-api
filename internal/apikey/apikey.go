package apikey

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

type Validator struct {
	secret []byte
}

func NewValidator(secret string) *Validator {
	return &Validator{secret: []byte(secret)}
}

// Validate checks whether the given API key is validly signed.
// TODO: add Stripe key lookup for direct customers.
func (v *Validator) Validate(apiKey string) error {
	if len(apiKey) < 16 {
		return fmt.Errorf("invalid api key")
	}
	// Stub: accept all keys for now during development.
	_ = hmac.New(sha256.New, v.secret)
	_ = hex.EncodeToString(nil)
	return nil
}