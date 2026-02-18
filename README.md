# Sales Tax API

A JSON API that returns jurisdiction-level US sales tax rates for any ZIP code or street address. Built for e-commerce platforms, POS systems, and accounting software that need accurate tax rates at checkout.

This is a rate lookup service only — not tax advice, filing, or exemption handling.

## API Endpoints

All endpoints return JSON. Authenticated routes require an API key.

### Public

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/v1/health` | Health check — reports API, database, and Redis status |

### Authenticated

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/v1/tax/zip/{zip_code}` | Tax rates for a 5-digit ZIP code. Returns combined rate, breakdown (state/county/city/special), and all matching jurisdictions |
| `GET` | `/v1/tax/address` | Tax rate for a street address. Query params: `street`, `city`, `state`, `zip` |
| `POST` | `/v1/tax/calculate` | Compute tax on an amount. Body: `{ "zip_code": "90210", "amount": 100.00 }` |
| `POST` | `/v1/tax/bulk` | Rates for up to 100 ZIP codes. Body: `{ "zip_codes": ["90210", "10001"] }` |

## Development Setup

### Prerequisites

- Go 1.22+
- Docker and Docker Compose
- [golang-migrate](https://github.com/golang-migrate/migrate) (`brew install golang-migrate`)

### Getting Started

```bash
# Start Postgres and Redis
docker compose up -d

# Run migrations (creates tables and seeds sample data)
migrate -path migrations -database "postgres://salestax:salestax@localhost:5432/salestax?sslmode=disable" up

# Start the server
DATABASE_URL="postgres://salestax:salestax@localhost:5432/salestax?sslmode=disable" \
REDIS_URL="redis://localhost:6379" \
API_KEY_SECRET="dev-secret-key-12345" \
go run cmd/server/main.go
```

### Test it

```bash
curl -H "X-API-Key: dev-key-1234567890" localhost:8080/v1/tax/zip/90210
```

### Sample Data

The seed migration includes 6 ZIP codes for testing:

| ZIP | Location | Combined Rate |
|-----|----------|--------------|
| `90210` | Beverly Hills, CA | 9.25% |
| `90001` | Los Angeles, CA | 8.00% |
| `10001` | Manhattan, NYC | 13.00% |
| `60601` | Chicago, IL | 10.25% |
| `77001` | Houston, TX | 8.25% |
| `97201` | Portland, OR | 0.00% |

### Tear down

```bash
# Reset the database
migrate -path migrations -database "postgres://salestax:salestax@localhost:5432/salestax?sslmode=disable" down

# Stop containers
docker compose down
```

## Configuration

All via environment variables:

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DATABASE_URL` | Yes | — | PostgreSQL connection string |
| `REDIS_URL` | Yes | — | Redis connection string |
| `API_KEY_SECRET` | Yes | — | HMAC secret for API key validation |
| `PORT` | No | `8080` | HTTP server port |
| `CACHE_TTL_HOURS` | No | `24` | Redis cache TTL |
| `RATE_LIMIT_RPS` | No | `10` | Requests per second per key |
| `LOG_LEVEL` | No | `info` | debug, info, warn, error |
| `ENVIRONMENT` | No | `production` | production, staging, development |
