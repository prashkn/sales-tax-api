-- Core tables

CREATE TABLE jurisdictions (
    fips_code       TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    type            TEXT NOT NULL CHECK (type IN ('state', 'county', 'city', 'special_district')),
    state_fips      TEXT NOT NULL,
    parent_fips     TEXT,
    effective_date  DATE NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE rates (
    id              SERIAL PRIMARY KEY,
    fips_code       TEXT NOT NULL REFERENCES jurisdictions(fips_code),
    rate            NUMERIC(7,5) NOT NULL CHECK (rate >= 0 AND rate <= 0.15),
    rate_type       TEXT NOT NULL DEFAULT 'general',
    effective_date  DATE NOT NULL,
    expiry_date     DATE,
    source          TEXT NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE zip_to_jurisdictions (
    zip_code        TEXT NOT NULL,
    fips_code       TEXT NOT NULL REFERENCES jurisdictions(fips_code),
    is_primary      BOOLEAN DEFAULT true,
    effective_date  DATE NOT NULL,
    expiry_date     DATE,
    PRIMARY KEY (zip_code, fips_code, effective_date)
);

CREATE TABLE rate_history (
    id              SERIAL PRIMARY KEY,
    fips_code       TEXT NOT NULL,
    old_rate        NUMERIC(7,5),
    new_rate        NUMERIC(7,5) NOT NULL,
    changed_date    DATE NOT NULL,
    source          TEXT NOT NULL,
    pipeline_run_id TEXT NOT NULL
);

-- Staging tables (used by pipeline, identical schema without constraints to parent tables)

CREATE TABLE jurisdictions_staging (
    fips_code       TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    type            TEXT NOT NULL CHECK (type IN ('state', 'county', 'city', 'special_district')),
    state_fips      TEXT NOT NULL,
    parent_fips     TEXT,
    effective_date  DATE NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE rates_staging (
    id              SERIAL PRIMARY KEY,
    fips_code       TEXT NOT NULL,
    rate            NUMERIC(7,5) NOT NULL CHECK (rate >= 0 AND rate <= 0.15),
    rate_type       TEXT NOT NULL DEFAULT 'general',
    effective_date  DATE NOT NULL,
    expiry_date     DATE,
    source          TEXT NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE zip_to_jurisdictions_staging (
    zip_code        TEXT NOT NULL,
    fips_code       TEXT NOT NULL,
    is_primary      BOOLEAN DEFAULT true,
    effective_date  DATE NOT NULL,
    expiry_date     DATE,
    PRIMARY KEY (zip_code, fips_code, effective_date)
);

-- Indexes

CREATE INDEX idx_zip_jurisdictions_zip ON zip_to_jurisdictions(zip_code) WHERE expiry_date IS NULL;
CREATE INDEX idx_rates_fips_active ON rates(fips_code) WHERE expiry_date IS NULL;
CREATE INDEX idx_rates_effective ON rates(fips_code, effective_date DESC);
