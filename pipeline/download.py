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


def _download_with_retry(
    client: httpx.Client,
    url: str,
    dest: Path,
    *,
    method: str = "GET",
    data: dict | None = None,
    expect_csv: bool = True,
) -> bool:
    """Download a URL to dest with exponential-backoff retries.

    Args:
        method: HTTP method ("GET" or "POST").
        data: Form data for POST requests.
        expect_csv: If True, validate that the response looks like CSV data
                    (not an HTML error page).

    Returns True on success, False on failure after all retries.
    """
    for attempt in range(MAX_RETRIES):
        try:
            if method == "POST" and data:
                resp = client.post(url, data=data, timeout=REQUEST_TIMEOUT, follow_redirects=True)
            else:
                resp = client.get(url, timeout=REQUEST_TIMEOUT, follow_redirects=True)
            resp.raise_for_status()

            content = resp.content
            if not content or len(content) < 10:
                logger.warning("Empty or near-empty response from %s (%d bytes)", url, len(content))
                return False

            # Validate the response looks like CSV, not an HTML error page.
            if expect_csv:
                text_start = content[:1024].decode("utf-8", errors="replace").strip().lower()
                if text_start.startswith("<!doctype") or text_start.startswith("<html"):
                    logger.warning(
                        "Response from %s appears to be HTML, not CSV — skipping. "
                        "First 200 chars: %s",
                        url, text_start[:200],
                    )
                    return False

            dest.write_bytes(content)
            logger.info("Downloaded %s (%d bytes)", dest.name, len(content))
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
    The public download requires a form POST to their download endpoint.
    If the POST approach fails (Avalara may change their form), the
    pipeline continues without Avalara data — SST and state_gov sources
    still provide coverage.

    NOTE: If Avalara changes their download flow, update AVALARA_DOWNLOAD_URL
    and the form payload below.  You can find the current form action by
    inspecting the page at AVALARA_RATE_URL.
    """
    out_dir = _quarter_dir() / "avalara"
    out_dir.mkdir(parents=True, exist_ok=True)
    downloaded: list[Path] = []

    # Avalara's download page requires a POST with form data to obtain the
    # actual CSV.  The direct CSV endpoint follows this pattern:
    download_url = "https://www.avalara.com/taxrates/en/download-tax-tables/download.html"
    form_data = {
        "format": "csv",
        "country": "US",
    }

    dest = out_dir / "avalara_tax_rates.csv"

    # Try the POST-based download first.
    if _download_with_retry(client, download_url, dest, method="POST", data=form_data):
        downloaded.append(dest)
    else:
        # Fallback: try the landing page URL directly (may return CSV in
        # some deployment environments or CDN configurations).
        logger.warning("Avalara POST download failed, trying direct GET as fallback")
        if _download_with_retry(client, AVALARA_RATE_URL, dest):
            downloaded.append(dest)
        else:
            logger.error(
                "Could not download Avalara tax tables via POST or GET. "
                "Non-SST states will lack Avalara coverage this quarter. "
                "Consider manually downloading from %s and placing the CSV at %s",
                AVALARA_RATE_URL, dest,
            )

    logger.info("Avalara: downloaded %d files", len(downloaded))
    return downloaded


# ---------------------------------------------------------------------------
# State government site downloads
# ---------------------------------------------------------------------------

# State-specific download configs.  Each entry maps a state abbreviation to
# a dict with a "url" key, an "expect_csv" flag (False for HTML pages that
# need manual conversion), and a human-readable "description".
#
# NOTE: Many state government sites serve HTML, not CSV.  Only entries with
# ``expect_csv: True`` will be validated as CSV on download.  States that
# serve HTML are included here for documentation but will typically fail the
# CSV content check and be skipped — operators should manually download and
# convert these to CSV, placing them in the raw/QUARTER/state_gov/ directory.
STATE_GOV_SOURCES: dict[str, dict] = {
    "CA": {
        # CDTFA publishes a downloadable CSV of district tax rates.
        "url": "https://www.cdtfa.ca.gov/taxes-and-fees/tax-rates-stfd.htm",
        "expect_csv": False,
        "description": "California CDTFA district tax rates — HTML page, requires manual CSV export",
    },
    "TX": {
        # Texas Comptroller publishes a downloadable CSV of local sales tax rates.
        "url": "https://comptroller.texas.gov/taxes/sales/rates.php",
        "expect_csv": False,
        "description": "Texas Comptroller rate table — HTML page, requires manual CSV export",
    },
    "NY": {
        # New York DTF Publication 718 is available as a CSV.
        "url": "https://www.tax.ny.gov/pdf/publications/sales/pub718.csv",
        "expect_csv": True,
        "description": "New York DTF Publication 718 — direct CSV download",
    },
    "FL": {
        # Florida DOR publishes a downloadable rate table.
        "url": "https://floridarevenue.com/taxes/taxesfees/Pages/tax_rate_table.aspx",
        "expect_csv": False,
        "description": "Florida DOR rate table — HTML page, requires manual CSV export",
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
        expect_csv = src.get("expect_csv", True)
        if _download_with_retry(client, src["url"], dest, expect_csv=expect_csv):
            downloaded.append(dest)
        elif not expect_csv:
            logger.info(
                "State gov %s serves HTML — place a manually converted CSV at %s "
                "before running the parse stage. Source: %s",
                abbr, dest, src.get("description", src["url"]),
            )

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
