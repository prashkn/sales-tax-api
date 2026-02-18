package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("connecting to database: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("pinging database: %w", err)
	}

	return &Store{pool: pool}, nil
}

func (s *Store) Close() {
	s.pool.Close()
}

func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

func (s *Store) GetJurisdictionsByZIP(ctx context.Context, zip string) ([]Jurisdiction, error) {
	query, args, err := jurisdictionsByZIPQuery(zip).ToSql()
	if err != nil {
		return nil, fmt.Errorf("building query: %w", err)
	}

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("querying jurisdictions: %w", err)
	}
	defer rows.Close()

	var jurisdictions []Jurisdiction
	for rows.Next() {
		var j Jurisdiction
		if err := rows.Scan(&j.FIPSCode, &j.Name, &j.Type, &j.StateFIPS, &j.ParentFIPS, &j.EffectiveDate); err != nil {
			return nil, fmt.Errorf("scanning jurisdiction: %w", err)
		}
		jurisdictions = append(jurisdictions, j)
	}
	return jurisdictions, rows.Err()
}

func (s *Store) GetRateByFIPS(ctx context.Context, fipsCode string) (*Rate, error) {
	query, args, err := rateByFIPSQuery(fipsCode).ToSql()
	if err != nil {
		return nil, fmt.Errorf("building query: %w", err)
	}

	var r Rate
	err = s.pool.QueryRow(ctx, query, args...).Scan(
		&r.ID, &r.FIPSCode, &r.Rate, &r.RateType, &r.EffectiveDate, &r.ExpiryDate, &r.Source,
	)
	if err != nil {
		return nil, fmt.Errorf("querying rate: %w", err)
	}
	return &r, nil
}

func (s *Store) GetRatesByFIPSCodes(ctx context.Context, fipsCodes []string) ([]Rate, error) {
	query, args, err := ratesByFIPSCodesQuery(fipsCodes).ToSql()
	if err != nil {
		return nil, fmt.Errorf("building query: %w", err)
	}

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("querying rates: %w", err)
	}
	defer rows.Close()

	var rates []Rate
	for rows.Next() {
		var r Rate
		if err := rows.Scan(&r.ID, &r.FIPSCode, &r.Rate, &r.RateType, &r.EffectiveDate, &r.ExpiryDate, &r.Source); err != nil {
			return nil, fmt.Errorf("scanning rate: %w", err)
		}
		rates = append(rates, r)
	}
	return rates, rows.Err()
}