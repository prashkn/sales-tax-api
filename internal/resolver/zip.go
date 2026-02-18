package resolver

import (
	"context"

	"github.com/prashkn/sales-tax-api/internal/store"
)

type ZIPResolver struct {
	store *store.Store
}

func NewZIPResolver(s *store.Store) *ZIPResolver {
	return &ZIPResolver{store: s}
}

func (r *ZIPResolver) Resolve(ctx context.Context, zipCode string) ([]store.Jurisdiction, error) {
	return r.store.GetJurisdictionsByZIP(ctx, zipCode)
}