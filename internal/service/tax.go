package service

import (
	"context"
	"fmt"

	"github.com/prashkn/sales-tax-api/internal/cache"
	"github.com/prashkn/sales-tax-api/internal/resolver"
	"github.com/prashkn/sales-tax-api/internal/store"
)

type TaxResponse struct {
	ZIPCode      string              `json:"zip_code"`
	CombinedRate float64             `json:"combined_rate"`
	Breakdown    RateBreakdown       `json:"breakdown"`
	Jurisdictions []JurisdictionRate `json:"jurisdictions"`
	Meta         Meta                `json:"meta"`
}

type RateBreakdown struct {
	State   float64 `json:"state"`
	County  float64 `json:"county"`
	City    float64 `json:"city"`
	Special float64 `json:"special"`
}

type JurisdictionRate struct {
	FIPSCode string  `json:"fips_code"`
	Name     string  `json:"name"`
	Type     string  `json:"type"`
	Rate     float64 `json:"rate"`
}

type Meta struct {
	LastUpdated string `json:"last_updated"`
	DataVersion string `json:"data_version"`
	Disclaimer  string `json:"disclaimer"`
}

type CalculateResponse struct {
	ZIPCode      string  `json:"zip_code"`
	Amount       float64 `json:"amount"`
	TaxRate      float64 `json:"tax_rate"`
	TaxAmount    float64 `json:"tax_amount"`
	Total        float64 `json:"total"`
	Meta         Meta    `json:"meta"`
}

type TaxService struct {
	zipResolver  *resolver.ZIPResolver
	addrResolver *resolver.AddressResolver
	rateResolver *resolver.RateResolver
	cache        *cache.Cache
}

func NewTaxService(s *store.Store, c *cache.Cache) *TaxService {
	return &TaxService{
		zipResolver:  resolver.NewZIPResolver(s),
		addrResolver: resolver.NewAddressResolver(s),
		rateResolver: resolver.NewRateResolver(s),
		cache:        c,
	}
}

func (ts *TaxService) LookupByZIP(ctx context.Context, zipCode string) (*TaxResponse, error) {
	// Try cache first.
	var cached TaxResponse
	if err := ts.cache.Get(ctx, zipCode, &cached); err == nil {
		return &cached, nil
	}

	jurisdictions, err := ts.zipResolver.Resolve(ctx, zipCode)
	if err != nil {
		return nil, fmt.Errorf("resolving zip: %w", err)
	}
	if len(jurisdictions) == 0 {
		return nil, fmt.Errorf("no jurisdictions found for zip %s", zipCode)
	}

	resp, err := ts.buildResponse(ctx, zipCode, jurisdictions)
	if err != nil {
		return nil, err
	}

	// Cache the result (best-effort).
	_ = ts.cache.Set(ctx, zipCode, resp)

	return resp, nil
}

func (ts *TaxService) LookupByAddress(ctx context.Context, street, city, state, zip string) (*TaxResponse, error) {
	jurisdictions, err := ts.addrResolver.Resolve(ctx, street, city, state, zip)
	if err != nil {
		return nil, fmt.Errorf("resolving address: %w", err)
	}
	if len(jurisdictions) == 0 {
		return nil, fmt.Errorf("no jurisdictions found for address")
	}
	return ts.buildResponse(ctx, zip, jurisdictions)
}

func (ts *TaxService) Calculate(ctx context.Context, zipCode string, amount float64) (*CalculateResponse, error) {
	taxResp, err := ts.LookupByZIP(ctx, zipCode)
	if err != nil {
		return nil, err
	}
	taxAmount := amount * taxResp.CombinedRate
	return &CalculateResponse{
		ZIPCode:   zipCode,
		Amount:    amount,
		TaxRate:   taxResp.CombinedRate,
		TaxAmount: taxAmount,
		Total:     amount + taxAmount,
		Meta:      taxResp.Meta,
	}, nil
}

func (ts *TaxService) buildResponse(ctx context.Context, zipCode string, jurisdictions []store.Jurisdiction) (*TaxResponse, error) {
	resp := &TaxResponse{
		ZIPCode: zipCode,
		Meta: Meta{
			Disclaimer: "For informational purposes only. Not tax advice. Verify with local tax authorities.",
		},
	}

	for _, j := range jurisdictions {
		rate, err := ts.rateResolver.GetRate(ctx, j.FIPSCode)
		if err != nil {
			continue // skip jurisdictions without active rates
		}

		jr := JurisdictionRate{
			FIPSCode: j.FIPSCode,
			Name:     j.Name,
			Type:     j.Type,
			Rate:     rate.Rate,
		}
		resp.Jurisdictions = append(resp.Jurisdictions, jr)

		switch j.Type {
		case "state":
			resp.Breakdown.State += rate.Rate
		case "county":
			resp.Breakdown.County += rate.Rate
		case "city":
			resp.Breakdown.City += rate.Rate
		case "special_district":
			resp.Breakdown.Special += rate.Rate
		}
	}

	resp.CombinedRate = resp.Breakdown.State + resp.Breakdown.County + resp.Breakdown.City + resp.Breakdown.Special
	return resp, nil
}