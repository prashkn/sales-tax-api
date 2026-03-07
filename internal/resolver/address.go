package resolver

import (
	"context"
	"log/slog"

	"github.com/prashkn/sales-tax-api/internal/geocoder"
	"github.com/prashkn/sales-tax-api/internal/store"
)

type AddressResolver struct {
	store    *store.Store
	geocoder *geocoder.Client
}

func NewAddressResolver(s *store.Store, gc *geocoder.Client) *AddressResolver {
	return &AddressResolver{store: s, geocoder: gc}
}

// Resolve geocodes an address to a precise set of jurisdictions.
// If a full street address is provided, it calls the Census Geocoder API to
// get exact FIPS codes. If geocoding fails or only a ZIP is provided, it
// falls back to the ZIP-based lookup.
func (r *AddressResolver) Resolve(ctx context.Context, street, city, state, zip string) ([]store.Jurisdiction, error) {
	// If we have a street address, attempt geocoding for precise resolution.
	if street != "" {
		result, err := r.geocoder.Geocode(ctx, street, city, state, zip)
		if err != nil {
			slog.Warn("geocoding failed, falling back to zip", "error", err, "zip", zip)
		} else if result != nil {
			jurisdictions, err := r.resolveFromGeocode(ctx, result)
			if err != nil {
				slog.Warn("fips lookup failed after geocode, falling back to zip", "error", err, "zip", zip)
			} else if len(jurisdictions) > 0 {
				return jurisdictions, nil
			}
		}
		// Geocoder returned no match — fall through to ZIP.
	}

	// Fall back to ZIP-based resolution.
	return r.store.GetJurisdictionsByZIP(ctx, zip)
}

// resolveFromGeocode builds a list of FIPS codes from the geocoder result
// and queries the jurisdictions table. The query also picks up any
// special_district children of the matched county.
func (r *AddressResolver) resolveFromGeocode(ctx context.Context, result *geocoder.Result) ([]store.Jurisdiction, error) {
	var fipsCodes []string

	if result.StateFIPS != "" {
		fipsCodes = append(fipsCodes, result.StateFIPS)
	}
	if result.CountyFIPS != "" {
		fipsCodes = append(fipsCodes, result.CountyFIPS)
	}
	if result.PlaceFIPS != "" {
		fipsCodes = append(fipsCodes, result.PlaceFIPS)
	}

	if len(fipsCodes) == 0 {
		return nil, nil
	}

	return r.store.GetJurisdictionsByFIPSCodes(ctx, fipsCodes)
}
