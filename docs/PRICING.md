# Pricing

The API is available through two channels: RapidAPI Hub and direct billing via Stripe.

---

## RapidAPI Hub

Distributed through [RapidAPI](https://rapidapi.com/). RapidAPI handles billing, API key provisioning, and usage metering. RapidAPI takes a 25% revenue share.

| Tier | Price | Requests/month | Bulk endpoint | Support |
|------|-------|----------------|---------------|---------|
| Free | $0 | 100 | No | Community |
| Basic | $9.99/mo | 5,000 | No | Email |
| Pro | $29.99/mo | 50,000 | Yes (up to 50 ZIPs/call) | Email |
| Business | $99.99/mo | 500,000 | Yes (up to 100 ZIPs/call) | Priority |

All tiers include:
- ZIP code lookup (`GET /v1/tax/zip/{zip}`)
- Address lookup (`GET /v1/tax/address`)
- Tax calculation (`POST /v1/tax/calculate`)
- Jurisdiction-level rate breakdowns
- 24-hour data freshness

---

## Direct API (Stripe Billing)

Available at our own domain. Stripe handles billing (3% transaction fee). API keys are HMAC-signed (`stx_` prefix) and provisioned on signup.

| Tier | Price | Requests/month | Bulk endpoint | Support |
|------|-------|----------------|---------------|---------|
| Starter | $14.99/mo | 10,000 | No | Email |
| Growth | $49.99/mo | 100,000 | Yes (up to 100 ZIPs/call) | Email |
| Scale | $149.99/mo | 1,000,000 | Yes (up to 100 ZIPs/call) | Priority |
| Enterprise | Custom | Unlimited | Yes | Dedicated |

### Overage

- Starter/Growth: Requests beyond the limit return HTTP 429. No overage billing.
- Scale: $0.0001 per request beyond the limit (metered billing).
- Enterprise: No limits.

---

## Revenue Strategy

The long-term goal is to shift the revenue mix from RapidAPI (75% net) toward direct billing (97% net). RapidAPI provides initial discovery and traffic; direct API provides better margins.

| Channel | Gross margin | Purpose |
|---------|-------------|---------|
| RapidAPI | 75% | Discovery, marketplace traffic |
| Direct | 97% | Retention, higher margin |
