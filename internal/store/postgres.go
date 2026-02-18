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
	rows, err := s.pool.Query(ctx, queryJurisdictionsByZIP, zip)
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
	var r Rate
	err := s.pool.QueryRow(ctx, queryRateByFIPS, fipsCode).Scan(
		&r.ID, &r.FIPSCode, &r.Rate, &r.RateType, &r.EffectiveDate, &r.ExpiryDate, &r.Source,
	)
	if err != nil {
		return nil, fmt.Errorf("querying rate: %w", err)
	}
	return &r, nil
}