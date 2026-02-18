package store

import "time"

type Jurisdiction struct {
	FIPSCode      string    `json:"fips_code"`
	Name          string    `json:"name"`
	Type          string    `json:"type"`
	StateFIPS     string    `json:"state_fips"`
	ParentFIPS    *string   `json:"parent_fips,omitempty"`
	EffectiveDate time.Time `json:"effective_date"`
}

type Rate struct {
	ID            int       `json:"id"`
	FIPSCode      string    `json:"fips_code"`
	Rate          float64   `json:"rate"`
	RateType      string    `json:"rate_type"`
	EffectiveDate time.Time `json:"effective_date"`
	ExpiryDate    *time.Time `json:"expiry_date,omitempty"`
	Source        string    `json:"source"`
}

type ZIPJurisdiction struct {
	ZIPCode       string    `json:"zip_code"`
	FIPSCode      string    `json:"fips_code"`
	IsPrimary     bool      `json:"is_primary"`
	EffectiveDate time.Time `json:"effective_date"`
	ExpiryDate    *time.Time `json:"expiry_date,omitempty"`
}
