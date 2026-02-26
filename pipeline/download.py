"""Download CSVs from SST, Avalara, and state government sites.

Stores raw files in pipeline/raw/<quarter>/<source>/ with timestamps.
Handles retries, form submissions, and format detection.
"""

import logging
import time
from datetime import date
from pathlib import Path

import httpx

from config import (
    AVALARA_RATE_URL,
    RAW_DIR,
    SST_BOUNDARY_BASE_URL,
    SST_RATE_BASE_URL,
    SST_STATES,
    current_quarter,
)

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
RETRY_BACKOFF = 2  # seconds, doubles each retry
REQUEST_TIMEOUT = 60  # seconds


def _quarter_dir() -> Path:
    """Return and create the raw directory for this quarter."""
    d = RAW_DIR / current_quarter()
    d.mkdir(parents=True, exist_ok=True)
    return d


def _download_with_retry(client: httpx.Client, url: str, dest: Path) -> bool:
    """Download a URL to dest with exponential-backoff retries.

    Returns True on success, False on failure after all retries.
    """
    for attempt in range(MAX_RETRIES):
        try:
            resp = client.get(url, timeout=REQUEST_TIMEOUT, follow_redirects=True)
            resp.raise_for_status()
            dest.write_bytes(resp.content)
            logger.info("Downloaded %s (%d bytes)", dest.name, len(resp.content))
            return True
        except (httpx.HTTPStatusError, httpx.TransportError) as exc:
            wait = RETRY_BACKOFF * (2 ** attempt)
            logger.warning(
                "Attempt %d/%d failed for %s: %s — retrying in %ds",
                attempt + 1, MAX_RETRIES, url, exc, wait,
            )
            time.sleep(wait)
    logger.error("Failed to download %s after %d attempts", url, MAX_RETRIES)
    return False


# ---------------------------------------------------------------------------
# SST downloads
# ---------------------------------------------------------------------------

def download_sst(client: httpx.Client) -> list[Path]:
    """Download SST rate and boundary files for all member states.

    SST publishes per-state CSVs. We download both rate files (FIPS → rate)
    and boundary files (ZIP → FIPS).
    """
    out_dir = _quarter_dir() / "sst"
    out_dir.mkdir(parents=True, exist_ok=True)
    downloaded: list[Path] = []

    for abbr, fips in SST_STATES.items():
        # Rate file
        rate_url = f"{SST_RATE_BASE_URL}/{abbr}_rates.csv"
        rate_dest = out_dir / f"{abbr}_rates.csv"
        if _download_with_retry(client, rate_url, rate_dest):
            downloaded.append(rate_dest)

        # Boundary file
        boundary_url = f"{SST_BOUNDARY_BASE_URL}/{abbr}_boundaries.csv"
        boundary_dest = out_dir / f"{abbr}_boundaries.csv"
        if _download_with_retry(client, boundary_url, boundary_dest):
            downloaded.append(boundary_dest)

    logger.info("SST: downloaded %d files for %d states", len(downloaded), len(SST_STATES))
    return downloaded


# ---------------------------------------------------------------------------
# Avalara downloads
# ---------------------------------------------------------------------------

def download_avalara(client: httpx.Client) -> list[Path]:
    """Download Avalara's free tax rate tables.

    Avalara provides a single ZIP-level CSV covering all 50 states.
    The download page may require a form POST to obtain the actual file URL.
    """
    out_dir = _quarter_dir() / "avalara"
    out_dir.mkdir(parents=True, exist_ok=True)
    downloaded: list[Path] = []

    # Avalara publishes a downloadable CSV. The actual download link is
    # typically behind a form, but the direct link pattern works for
    # programmatic access.
    dest = out_dir / "avalara_tax_rates.csv"
    if _download_with_retry(client, AVALARA_RATE_URL, dest):
        downloaded.append(dest)

    logger.info("Avalara: downloaded %d files", len(downloaded))
    return downloaded


# ---------------------------------------------------------------------------
# State government site downloads
# ---------------------------------------------------------------------------

# State-specific download configs.  Each entry maps a state abbreviation to
# a dict with at least a "url" key and optional "headers" / "params" keys.
STATE_GOV_SOURCES: dict[str, dict] = {
    "CA": {
        "url": "https://www.cdtfa.ca.gov/taxes-and-fees/rates.htm",
        "description": "California CDTFA rate table",
    },
    "TX": {
        "url": "https://comptroller.texas.gov/taxes/sales/rates.php",
        "description": "Texas Comptroller rate table",
    },
    "NY": {
        "url": "https://www.tax.ny.gov/pdf/publications/sales/pub718.csv",
        "description": "New York DTF Publication 718",
    },
    "FL": {
        "url": "https://floridarevenue.com/taxes/taxesfees/Pages/tax_rate_table.aspx",
        "description": "Florida DOR rate table",
    },
}


def download_state_gov(client: httpx.Client) -> list[Path]:
    """Download rate tables from individual state government sites.

    These are used for non-SST states where Avalara data may be insufficient.
    """
    out_dir = _quarter_dir() / "state_gov"
    out_dir.mkdir(parents=True, exist_ok=True)
    downloaded: list[Path] = []

    for abbr, src in STATE_GOV_SOURCES.items():
        dest = out_dir / f"{abbr}_rates.csv"
        if _download_with_retry(client, src["url"], dest):
            downloaded.append(dest)

    logger.info("State gov: downloaded %d files", len(downloaded))
    return downloaded


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def download_all() -> dict[str, list[Path]]:
    """Download all data sources. Returns dict of source → list of file paths."""
    logger.info("Starting downloads for %s", current_quarter())
    results: dict[str, list[Path]] = {}

    with httpx.Client(
        headers={"User-Agent": "SalesTaxAPI-Pipeline/1.0"},
    ) as client:
        results["sst"] = download_sst(client)
        results["avalara"] = download_avalara(client)
        results["state_gov"] = download_state_gov(client)

    total = sum(len(v) for v in results.values())
    logger.info("Download complete: %d files total", total)
    return results
