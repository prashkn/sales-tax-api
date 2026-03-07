"""Pipeline configuration: source URLs, DB connection, file paths, constants."""

import os
from pathlib import Path
from datetime import date

# --- Directories ---

BASE_DIR = Path(__file__).resolve().parent
RAW_DIR = BASE_DIR / "raw"
REPORTS_DIR = BASE_DIR / "reports"

# --- Database ---

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/salestax")

# --- Current quarter ---

def current_quarter() -> str:
    """Return label like '2026-Q1'."""
    today = date.today()
    q = (today.month - 1) // 3 + 1
    return f"{today.year}-Q{q}"

# --- Data source URLs ---

# SST rate and boundary files (24 member states).
# The SST publishes ZIP-level boundary-to-rate CSVs per state.
SST_RATE_BASE_URL = "https://www.streamlinedsalestax.org/otm/tax-rates-and-boundaries"
SST_BOUNDARY_BASE_URL = "https://www.streamlinedsalestax.org/otm/boundary-database"

# SST member state FIPS codes and abbreviations.
SST_STATES = {
    "AR": "05", "GA": "13", "IN": "18", "IA": "19", "KS": "20",
    "KY": "21", "MI": "26", "MN": "27", "NE": "31", "NV": "32",
    "NJ": "34", "NC": "37", "ND": "38", "OH": "39", "OK": "40",
    "RI": "44", "SD": "46", "TN": "47", "UT": "49", "VT": "50",
    "WA": "53", "WV": "54", "WI": "55", "WY": "56",
}

# Avalara free tax rate tables — covers all 50 states at ZIP level.
AVALARA_RATE_URL = "https://www.avalara.com/taxrates/en/download-tax-tables.html"

# --- Validation thresholds ---

MIN_RATE = 0.0
MAX_RATE = 0.15          # 15% ceiling — anything above is flagged
MAX_RATE_DELTA = 0.02    # flag quarter-over-quarter changes > 2 pp
MIN_ZIP_COVERAGE = 40000 # US has ~41k 5-digit ZIPs; flag if we cover fewer

# --- Source priority (higher number wins when merging) ---

SOURCE_PRIORITY = {
    "avalara": 1,
    "state_gov": 2,
    "sst": 3,
}

# --- Pipeline run ID format ---

def pipeline_run_id() -> str:
    """Generate a unique run ID for this pipeline execution."""
    today = date.today()
    return f"pipeline-{today.isoformat()}-{current_quarter()}"
