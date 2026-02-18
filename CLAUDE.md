# CLAUDE.md

## Project Overview

This is a **US Sales Tax Rate Lookup API** — a commercial API product that returns accurate, jurisdiction-level sales tax rates for any US ZIP code or street address. It is monetized through RapidAPI Hub and a direct Stripe-billed API on our own domain.

The API serves e-commerce platforms, POS systems, accounting software, and any application that needs to calculate US sales tax at checkout. Buyers are businesses, not hobbyists.

**This is not tax advice software.** Every API response includes a disclaimer field. We provide rate lookups only — no filing, no exemption handling, no nexus determination.

---

## Architecture

The system has two completely independent subsystems that share PostgreSQL as their only interface:

```
┌─────────────────────────────────────────────────────┐
│                   API Consumers                      │
│         (RapidAPI Hub / Direct API keys)             │
└────────────────────┬────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────┐
│                 API Server (Go)                      │
│  Chi router · middleware · handlers · rate limiter   │
│                     │                                │
│              ┌──────┴──────┐                         │
│              ▼             ▼                          │
│          Redis          PostgreSQL                    │
│        (cache)        (source of truth)               │
└─────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────┐
│             Data Pipeline (Python)                    │
│  Download CSVs · parse · normalize · validate        │
│                     │                                │
│                     ▼                                │
│               PostgreSQL                             │
│          (staging → production)                       │
└─────────────────────────────────────────────────────┘
```

The Go server and Python pipeline never communicate directly. The pipeline writes to Postgres. The API reads from Postgres (and Redis cache). They are deployed and run independently.

---

## Tech Stack

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| API server | Go | 1.22+ | HTTP JSON API, all request handling |
| Router | chi (go-chi/chi/v5) | v5 | Lightweight HTTP router with middleware |
| Database | PostgreSQL | 16 | Source of truth for all tax rate data |
| Cache | Redis | 7 | ZIP code lookup caching, 24-hour TTL |
| Go Postgres driver | pgx (jackc/pgx/v5) | v5 | Connection pooling, prepared statements |
| Go Redis driver | go-redis (redis/go-redis/v9) | v9 | Cache reads/writes |
| Data pipeline | Python | 3.11+ | CSV ingestion, normalization, validation |
| Pipeline dependencies | pandas, httpx, psycopg2 | latest | CSV wrangling, HTTP downloads, DB writes |
| Hosting | Fly.io | — | Go API deployed as Docker container |
| Pipeline runner | GitHub Actions | — | Scheduled quarterly cron + manual trigger |
| Direct billing | Stripe | — | API key management and billing for direct customers |
| Monitoring | BetterStack (uptime), Sentry (errors) | — | Alerting on downtime, error spikes, data staleness |

---

## Directory Structure

```
/
├── CLAUDE.md                  # This file
├── README.md                  # Public-facing documentation
├── go.mod                     # Go module definition
├── go.sum
├── Dockerfile                 # Multi-stage build for Go API
├── fly.toml                   # Fly.io deployment config
├── .github/
│   └── workflows/
│       ├── deploy.yml         # CI/CD: test → build → deploy Go API
│       └── pipeline.yml       # Scheduled quarterly data pipeline run
│
├── cmd/
│   └── server/
│       └── main.go            # Entrypoint: config loading, server startup, graceful shutdown
│
├── internal/
│   ├── config/
│   │   └── config.go          # Environment variable parsing, defaults, validation
│   │
│   ├── handler/
│   │   ├── tax.go             # HTTP handlers: GET /v1/tax/zip/{zip}, GET /v1/tax/address, POST /v1/tax/calculate, POST /v1/tax/bulk
│   │   ├── health.go          # GET /v1/health — uptime check, DB connectivity, data freshness
│   │   └── middleware.go      # Rate limiting, API key auth, request logging, CORS
│   │
│   ├── service/
│   │   └── tax.go             # Business logic: resolve ZIP/address → jurisdictions → rates → response
│   │
│   ├── resolver/
│   │   ├── zip.go             # ZIP code → FIPS code(s) resolution, handles multi-jurisdiction ZIPs
│   │   ├── address.go         # Full street address → lat/lng → precise jurisdiction (future: geocoder integration)
│   │   └── rate.go            # FIPS code(s) → rate lookup, sums state + county + city + special district
│   │
│   ├── cache/
│   │   └── redis.go           # Redis get/set with 24h TTL, cache key format: "tax:zip:{zip_code}", fallback to DB on miss
│   │
│   ├── store/
│   │   ├── postgres.go        # Database connection pool setup (pgxpool)
│   │   ├── queries.go         # All SQL queries as constants or sqlc-generated code
│   │   └── models.go          # Go structs matching DB tables
│   │
│   └── apikey/
│       └── apikey.go          # API key validation for direct (non-RapidAPI) customers, Stripe key lookup
│
├── migrations/
│   ├── 001_create_tables.up.sql
│   ├── 001_create_tables.down.sql
│   └── ...                    # golang-migrate compatible migration files
│
├── pipeline/                  # Python data pipeline (entirely separate from Go code)
│   ├── requirements.txt       # pandas, httpx, psycopg2-binary, click
│   ├── config.py              # Pipeline configuration: source URLs, DB connection, file paths
│   ├── download.py            # Download CSVs from SST, Avalara, state gov sites
│   ├── parse.py               # Parse and normalize heterogeneous CSV formats into unified schema
│   ├── validate.py            # Sanity checks: rate bounds (0-15%), coverage (all ZIPs present), anomaly detection
│   ├── diff.py                # Compare staging data against current production, generate human-readable diff report
│   ├── load.py                # Upsert validated data from staging tables into production tables
│   └── run.py                 # CLI entrypoint: orchestrates download → parse → validate → diff → load
│
└── docs/
    ├── openapi.yaml           # OpenAPI 3.0 spec — the authoritative API contract
    ├── PRICING.md             # Pricing tier definitions for RapidAPI and direct billing
    └── DATA_SOURCES.md        # Documentation of every data source, its format, update frequency, and quirks
```

---

## Database Schema

```sql
-- Core tables

CREATE TABLE jurisdictions (
    fips_code       TEXT PRIMARY KEY,        -- Federal Information Processing Standard code
    name            TEXT NOT NULL,           -- e.g., "Los Angeles County"
    type            TEXT NOT NULL,           -- 'state' | 'county' | 'city' | 'special_district'
    state_fips      TEXT NOT NULL,           -- 2-digit state FIPS
    parent_fips     TEXT,                    -- parent jurisdiction (county's state, city's county)
    effective_date  DATE NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE rates (
    id              SERIAL PRIMARY KEY,
    fips_code       TEXT NOT NULL REFERENCES jurisdictions(fips_code),
    rate            NUMERIC(7,5) NOT NULL,   -- e.g., 0.08250 for 8.25%
    rate_type       TEXT NOT NULL,           -- 'general' | 'food' | 'clothing' (future)
    effective_date  DATE NOT NULL,
    expiry_date     DATE,                   -- NULL means currently active
    source          TEXT NOT NULL,           -- 'sst' | 'avalara' | 'state_gov' | 'manual'
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE zip_to_jurisdictions (
    zip_code        TEXT NOT NULL,           -- 5-digit ZIP
    fips_code       TEXT NOT NULL REFERENCES jurisdictions(fips_code),
    is_primary      BOOLEAN DEFAULT true,    -- for ZIPs spanning multiple jurisdictions
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
    pipeline_run_id TEXT NOT NULL            -- links to the pipeline execution that made this change
);

-- Staging tables (used by pipeline, identical schema with _staging suffix)
CREATE TABLE jurisdictions_staging (LIKE jurisdictions INCLUDING ALL);
CREATE TABLE rates_staging (LIKE rates INCLUDING ALL);
CREATE TABLE zip_to_jurisdictions_staging (LIKE zip_to_jurisdictions INCLUDING ALL);

-- Indexes
CREATE INDEX idx_zip_jurisdictions_zip ON zip_to_jurisdictions(zip_code) WHERE expiry_date IS NULL;
CREATE INDEX idx_rates_fips_active ON rates(fips_code) WHERE expiry_date IS NULL;
CREATE INDEX idx_rates_effective ON rates(fips_code, effective_date DESC);
```

---

## API Endpoints

All endpoints return JSON. All responses include a `meta` object with `last_updated`, `data_version`, and `disclaimer`.

### `GET /v1/tax/zip/{zip_code}`
Returns all tax jurisdictions and rates for a 5-digit ZIP code. Because a single ZIP can span multiple jurisdictions, the response may contain multiple rate breakdowns. Returns the most common (primary) rate as `combined_rate` and all possible rates in a `jurisdictions` array.

**Response shape:**
```json
{
  "zip_code": "90210",
  "combined_rate": 0.0950,
  "breakdown": {
    "state": 0.0725,
    "county": 0.0100,
    "city": 0.0125,
    "special": 0.0000
  },
  "jurisdictions": [
    {
      "fips_code": "0603744000",
      "name": "Beverly Hills",
      "type": "city",
      "rate": 0.0125
    }
  ],
  "meta": {
    "last_updated": "2026-01-15",
    "data_version": "2026-Q1",
    "disclaimer": "For informational purposes only. Not tax advice. Verify with local tax authorities."
  }
}
```

### `GET /v1/tax/address`
Query params: `street`, `city`, `state`, `zip`. Returns a single precise rate by geocoding the address to an exact jurisdiction. Uses a geocoding service (future: Census Geocoder API, free, no key required) to resolve address → lat/lng → FIPS block → exact jurisdiction.

### `POST /v1/tax/calculate`
Body: `{ "zip_code": "90210", "amount": 100.00 }`. Returns the tax amount and total. Convenience endpoint that wraps the ZIP lookup.

### `POST /v1/tax/bulk`
Body: `{ "zip_codes": ["90210", "10001", "60601"] }`. Returns rates for up to 100 ZIP codes in a single call. Paid tiers only.

### `GET /v1/health`
Returns API status, database connectivity, Redis connectivity, and data freshness (how old the latest rate data is). Used by monitoring.

---

## Data Pipeline

The pipeline runs quarterly (January, April, July, October) aligned with SST rate and boundary file release dates. It can also be triggered manually via GitHub Actions.

### Data Sources (in priority order)

1. **Streamlined Sales Tax (SST) rate and boundary files** — `streamlinedsalestax.org/Shared-Pages/rate-and-boundary-files`. Covers 24 member states. Machine-readable CSVs with FIPS codes. This is the highest-quality source. Boundary files map ZIP codes (5-digit and 9-digit) to jurisdiction FIPS codes. Rate files map FIPS codes to tax rates.

2. **Avalara free CSV rate tables** — `avalara.com/taxrates/en/download-tax-tables.html`. Covers all 50 states. Requires form submission to download. Lower granularity than SST files but provides baseline coverage for non-SST states (CA, TX, NY, FL, etc.).

3. **Individual state government sites** — For states where SST and Avalara data is insufficient or stale. Each state has its own format and quirks. Documented in `docs/DATA_SOURCES.md`.

### Pipeline Stages

1. **Download** (`download.py`): Fetch all source CSVs. Handle form submissions, retries, format detection. Store raw files in `pipeline/raw/` with timestamps.

2. **Parse** (`parse.py`): Each source has a dedicated parser function. Normalize all data into a unified schema matching the staging tables. Handle encoding issues, inconsistent column names, missing fields.

3. **Validate** (`validate.py`): Sanity checks before loading:
   - Every rate is between 0.0 and 0.15 (0% to 15%). Flag anything outside this range.
   - Every US ZIP code (USPS dataset, ~41,000 ZIPs) has at least one jurisdiction mapping.
   - No jurisdiction has a NULL or negative rate.
   - Compare against previous quarter: flag any rate that changed by more than 2 percentage points (likely data error, not real rate change).

4. **Diff** (`diff.py`): Generate a human-readable report of all changes: new jurisdictions, removed jurisdictions, rate increases, rate decreases. This is reviewed manually before the load step is approved.

5. **Load** (`load.py`): Write validated data to staging tables. After manual review of the diff report, promote staging → production via table swap or merge upsert. Record all changes in `rate_history`.

### Pipeline Safety

- The pipeline NEVER writes directly to production tables. It always writes to `_staging` tables first.
- The load step requires explicit confirmation (manual approval in GitHub Actions) after reviewing the diff report.
- All pipeline runs are logged with a `pipeline_run_id` for auditability.
- Raw source files are retained for 4 quarters for debugging.

---

## Deployment

### Go API (Fly.io)

- Multi-stage Docker build: build Go binary in golang:1.22-alpine, copy to scratch/distroless.
- Deployed via `fly deploy` from GitHub Actions on push to `main`.
- Environment variables: `DATABASE_URL`, `REDIS_URL`, `SENTRY_DSN`, `API_KEY_SECRET` (for signing direct API keys).
- Health check: Fly.io hits `GET /v1/health` every 30 seconds.
- Scaling: Start with 1 shared-cpu-1x machine (256MB). Scale up when traffic justifies it.

### PostgreSQL

- Fly.io Postgres or Supabase. Single instance is fine for the foreseeable future.
- Run migrations with `golang-migrate` on deploy.

### Redis

- Upstash (serverless Redis, free tier: 10K commands/day, more than enough for early stage).
- Or Fly.io Redis if co-located for lower latency.

### Data Pipeline (GitHub Actions)

- Scheduled cron: `0 9 1 1,4,7,10 *` (9am UTC on the 1st of Jan/Apr/Jul/Oct).
- Can also be triggered manually via `workflow_dispatch`.
- Uses a Python 3.11 runner. Installs `pipeline/requirements.txt`.
- Connects to production Postgres via `DATABASE_URL` secret.
- The diff report is posted as a GitHub Actions artifact for review.
- The production load step is a separate job that requires manual approval via GitHub Environments.

---

## Configuration

All configuration is via environment variables. No config files.

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `PORT` | No | `8080` | HTTP server port |
| `DATABASE_URL` | Yes | — | PostgreSQL connection string |
| `REDIS_URL` | Yes | — | Redis connection string |
| `SENTRY_DSN` | No | — | Sentry error tracking DSN |
| `API_KEY_SECRET` | Yes | — | HMAC secret for signing/validating direct API keys |
| `RATE_LIMIT_RPS` | No | `10` | Default requests per second per API key |
| `CACHE_TTL_HOURS` | No | `24` | Redis cache TTL in hours |
| `LOG_LEVEL` | No | `info` | Logging level: debug, info, warn, error |
| `ENVIRONMENT` | No | `production` | production, staging, development |

---

## Key Design Decisions

1. **Go for API, Python for pipeline.** The API is a performance-critical hot path — Go gives us sub-5ms response times on cached lookups and low memory usage. The pipeline is a quarterly batch job parsing messy CSVs — Python/pandas is dramatically more productive for this work.

2. **ZIP codes can map to multiple jurisdictions.** A single 5-digit ZIP can straddle county or city boundaries. We store all possible mappings and return all of them, with `is_primary` indicating the most common one. This is more accurate than competitors who return a single rate per ZIP.

3. **Staging tables with manual approval.** Tax rate data directly affects business transactions. We never auto-promote pipeline output to production. A human reviews the diff report first.

4. **Redis cache with 24-hour TTL.** Tax rates change at most quarterly. A 24-hour TTL means 95%+ cache hit rate with negligible staleness risk. On cache miss, the DB query is still fast (indexed lookups, ~5-10ms).

5. **Dual distribution: RapidAPI + direct.** RapidAPI provides discovery but takes 25%. Direct customers via our own domain pay through Stripe (3% fees). Over time, the goal is to shift the revenue mix toward direct.

6. **OpenAPI spec is the contract.** The `docs/openapi.yaml` file is the authoritative definition of all endpoints, request/response shapes, and error codes. Handlers are implemented to match this spec exactly.

---

## Common Tasks

### Run the API locally
```bash
# Start dependencies
docker compose up -d postgres redis

# Run migrations
migrate -path migrations -database "$DATABASE_URL" up

# Run the server
go run cmd/server/main.go
```

### Run the data pipeline locally
```bash
cd pipeline
pip install -r requirements.txt
python run.py --stage download
python run.py --stage parse
python run.py --stage validate
python run.py --stage diff       # review output before proceeding
python run.py --stage load       # writes to staging tables
python run.py --stage promote    # swaps staging → production (requires --confirm flag)
```

### Add a new data source
1. Add source metadata to `pipeline/config.py`
2. Write a parser function in `pipeline/parse.py` that outputs the unified schema
3. Add the download logic to `pipeline/download.py`
4. Document the source format and quirks in `docs/DATA_SOURCES.md`
5. Run the full pipeline against staging and verify with `validate.py`

### Deploy
```bash
fly deploy    # or push to main, CI/CD handles it
```
