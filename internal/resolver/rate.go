package resolver

import (
	"context"

	"github.com/prashkn/sales-tax-api/internal/store"
)

type RateResolver struct {
	store *store.Store
}

func NewRateResolver(s *store.Store) *RateResolver {
	return &RateResolver{store: s}
}

func (r *RateResolver) GetRate(ctx context.Context, fipsCode string) (*store.Rate, error) {
	return r.store.GetRateByFIPS(ctx, fipsCode)
}