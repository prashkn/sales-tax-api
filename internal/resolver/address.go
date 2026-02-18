package resolver

import (
	"context"
	"fmt"

	"github.com/prashkn/sales-tax-api/internal/store"
)

type AddressResolver struct {
	store *store.Store
}

func NewAddressResolver(s *store.Store) *AddressResolver {
	return &AddressResolver{store: s}
}

// Resolve geocodes an address to a precise jurisdiction.
// TODO: integrate Census Geocoder API for lat/lng -> FIPS resolution.
func (r *AddressResolver) Resolve(ctx context.Context, street, city, state, zip string) ([]store.Jurisdiction, error) {
	if zip == "" {
		return nil, fmt.Errorf("zip code is required for address resolution")
	}
	// For now, fall back to ZIP-based resolution.
	return r.store.GetJurisdictionsByZIP(ctx, zip)
}