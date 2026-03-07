# Data Sources

This document describes every data source used by the pipeline, its format, update frequency, and known quirks.

---

## 1. Streamlined Sales Tax (SST)

**Priority:** Highest (3)
**Coverage:** 24 member states
**URL:** `streamlinedsalestax.org/otm/`
**Format:** CSV (rate files + boundary files per state)
**Update frequency:** Quarterly (aligned with Jan/Apr/Jul/Oct)

### Rate files

One CSV per state. Columns include FIPS code, jurisdiction name, jurisdiction type, and tax rate. Rates are broken down by state, county, city, and special district.

### Boundary files

One CSV per state. Maps 5-digit and 9-digit ZIP codes to jurisdiction FIPS codes. A single ZIP can map to multiple FIPS codes when it straddles jurisdiction boundaries.

### Member states

AR, GA, IN, IA, KS, KY, MI, MN, NE, NV, NJ, NC, ND, OH, OK, RI, SD, TN, UT, VT, WA, WV, WI, WY

### Quirks

- Some states publish 9-digit ZIP boundaries; we truncate to 5-digit.
- Column names vary slightly between states (e.g., `FIPSCode` vs `FIPS_Code`). The parser uses heuristic column detection.
- Boundary file updates sometimes lag rate file updates by a few days.

---

## 2. Avalara Free Tax Rate Tables

**Priority:** Low (1)
**Coverage:** All 50 states + DC
**URL:** `avalara.com/taxrates/en/download-tax-tables.html`
**Format:** Single CSV covering all states
**Update frequency:** Varies; typically monthly refreshes
**Download method:** Form POST required (see `download.py` for details)

### Columns

| Column | Description |
|--------|-------------|
| State | 2-letter abbreviation |
| ZipCode | 5-digit ZIP |
| TaxRegionName | Human-readable jurisdiction name |
| TaxRegionCode | Avalara's internal region code |
| EstimatedCombinedRate | Total rate (state + local) |
| EstimatedStateRate | State-level rate |
| EstimatedCountyRate | County-level rate |
| EstimatedCityRate | City-level rate |
| EstimatedSpecialRate | Special district rate |

### Quirks

- Avalara does not provide real FIPS codes for sub-state jurisdictions. The pipeline synthesizes `AVL`-prefixed identifiers (e.g., `AVL0690210`).
- For SST-covered states, Avalara data is redundant and lower quality. The merge step filters out Avalara synthetic jurisdictions for those states.
- The download page requires a form POST. If Avalara changes their form, the download will fail gracefully and log instructions for manual download.
- Rates labeled "Estimated" — Avalara's free tier doesn't guarantee precision.

---

## 3. State Government Sites

**Priority:** Medium (2)
**Coverage:** Individual non-SST states (CA, TX, NY, FL)

### California (CDTFA)

- **URL:** `cdtfa.ca.gov/taxes-and-fees/tax-rates-stfd.htm`
- **Format:** HTML page — requires manual CSV export
- **Frequency:** Updated quarterly
- **Notes:** District tax rates. The page lists all special taxing districts with their rates and effective dates.

### Texas (Comptroller)

- **URL:** `comptroller.texas.gov/taxes/sales/rates.php`
- **Format:** HTML page — requires manual CSV export
- **Frequency:** Updated quarterly
- **Notes:** Local sales tax rates by city and county.

### New York (DTF)

- **URL:** `tax.ny.gov/pdf/publications/sales/pub718.csv`
- **Format:** Direct CSV download
- **Frequency:** Updated quarterly (Publication 718)
- **Notes:** Jurisdiction rates with FIPS-like codes. The CSV may use Windows-1252 encoding.

### Florida (DOR)

- **URL:** `floridarevenue.com/taxes/taxesfees/Pages/tax_rate_table.aspx`
- **Format:** HTML page — requires manual CSV export
- **Frequency:** Updated semi-annually
- **Notes:** Discretionary sales surtax rates by county.

### Quirks (all state sources)

- Most state sites serve HTML, not CSV. The pipeline logs instructions for manual conversion when HTML is detected.
- Column names and formats vary per state. The `parse_state_gov()` function uses heuristic column detection.
- State gov data uses `SGV`-prefixed synthetic FIPS codes for local jurisdictions.
- Files should be placed in `pipeline/raw/<quarter>/state_gov/<STATE>_rates.csv` if downloaded manually.

---

## Source Priority

When the same jurisdiction appears in multiple sources, the highest-priority source wins:

| Priority | Source | Rationale |
|----------|--------|-----------|
| 3 (highest) | SST | Official, machine-readable, real FIPS codes |
| 2 | State gov | Authoritative for the specific state |
| 1 (lowest) | Avalara | Broadest coverage but estimated rates, synthetic FIPS |

---

## Adding a New Source

1. Add source metadata to `pipeline/config.py`
2. Write download logic in `pipeline/download.py`
3. Write a parser in `pipeline/parse.py` that outputs the unified schema
4. Add a priority entry in `config.SOURCE_PRIORITY`
5. Document the source here
6. Run the full pipeline against staging and verify with `validate.py`
