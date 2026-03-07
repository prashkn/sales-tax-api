"""Sanity checks on parsed data before loading into staging tables.

Checks:
  - Every rate is within [0%, 15%].
  - ZIP coverage meets the minimum threshold (~41k US ZIPs).
  - No jurisdiction has a NULL or negative rate.
  - Quarter-over-quarter rate changes > 2 percentage points are flagged.
"""

import logging
from dataclasses import dataclass, field

import pandas as pd
import psycopg2

from config import (
    DATABASE_URL,
    MAX_RATE,
    MAX_RATE_DELTA,
    MIN_RATE,
    MIN_ZIP_COVERAGE,
)

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Collects warnings and errors from validation checks."""

    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return len(self.errors) == 0

    def summary(self) -> str:
        lines = []
        if self.errors:
            lines.append(f"ERRORS ({len(self.errors)}):")
            for e in self.errors:
                lines.append(f"  ✗ {e}")
        if self.warnings:
            lines.append(f"WARNINGS ({len(self.warnings)}):")
            for w in self.warnings:
                lines.append(f"  ⚠ {w}")
        if self.passed and not self.warnings:
            lines.append("All validation checks passed.")
        return "\n".join(lines)


def validate(
    jurisdictions: pd.DataFrame,
    rates: pd.DataFrame,
    zip_to_jurisdictions: pd.DataFrame,
) -> ValidationResult:
    """Run all validation checks against the parsed DataFrames.

    Returns a ValidationResult. If result.passed is False, the data
    should NOT be loaded into staging.
    """
    result = ValidationResult()

    _check_empty(jurisdictions, rates, zip_to_jurisdictions, result)
    _check_rate_bounds(rates, result)
    _check_null_rates(rates, result)
    _check_zip_coverage(zip_to_jurisdictions, result)
    _check_orphan_rates(jurisdictions, rates, result)
    _check_orphan_zip_mappings(jurisdictions, zip_to_jurisdictions, result)
    _check_rate_deltas(rates, result)

    logger.info("Validation %s: %d errors, %d warnings",
                "PASSED" if result.passed else "FAILED",
                len(result.errors), len(result.warnings))
    return result


# ---------------------------------------------------------------------------
# Individual checks
# ---------------------------------------------------------------------------

def _check_empty(
    jurisdictions: pd.DataFrame,
    rates: pd.DataFrame,
    zips: pd.DataFrame,
    result: ValidationResult,
) -> None:
    """Fail if any of the core DataFrames are empty."""
    if jurisdictions.empty:
        result.errors.append("Jurisdictions DataFrame is empty — no data parsed.")
    if rates.empty:
        result.errors.append("Rates DataFrame is empty — no data parsed.")
    if zips.empty:
        result.errors.append("ZIP-to-jurisdictions DataFrame is empty — no data parsed.")


def _check_rate_bounds(rates: pd.DataFrame, result: ValidationResult) -> None:
    """Every rate must be within [MIN_RATE, MAX_RATE]."""
    if rates.empty:
        return

    below = rates[rates["rate"] < MIN_RATE]
    above = rates[rates["rate"] > MAX_RATE]

    if not below.empty:
        result.errors.append(
            f"{len(below)} rates below {MIN_RATE:.1%}: "
            f"{below['fips_code'].head(5).tolist()}"
        )

    if not above.empty:
        result.errors.append(
            f"{len(above)} rates above {MAX_RATE:.1%}: "
            f"{above['fips_code'].head(5).tolist()}"
        )


def _check_null_rates(rates: pd.DataFrame, result: ValidationResult) -> None:
    """No rate should be NaN/NULL."""
    if rates.empty:
        return

    nulls = rates[rates["rate"].isna()]
    if not nulls.empty:
        result.errors.append(
            f"{len(nulls)} jurisdictions have NULL rates: "
            f"{nulls['fips_code'].head(5).tolist()}"
        )


def _check_zip_coverage(zips: pd.DataFrame, result: ValidationResult) -> None:
    """We should cover at least MIN_ZIP_COVERAGE unique 5-digit ZIP codes."""
    if zips.empty:
        return

    unique_zips = zips["zip_code"].nunique()
    if unique_zips < MIN_ZIP_COVERAGE:
        result.warnings.append(
            f"Only {unique_zips:,} unique ZIP codes covered "
            f"(minimum expected: {MIN_ZIP_COVERAGE:,}). "
            f"Some ZIPs may be missing data."
        )
    else:
        logger.info("ZIP coverage: %d unique ZIPs (threshold: %d)", unique_zips, MIN_ZIP_COVERAGE)


def _check_orphan_rates(
    jurisdictions: pd.DataFrame,
    rates: pd.DataFrame,
    result: ValidationResult,
) -> None:
    """Warn if any rate references a fips_code not in jurisdictions."""
    if rates.empty or jurisdictions.empty:
        return

    known_fips = set(jurisdictions["fips_code"])
    orphan_mask = ~rates["fips_code"].isin(known_fips)
    orphans = rates[orphan_mask]

    if not orphans.empty:
        result.warnings.append(
            f"{len(orphans)} rates reference unknown jurisdictions: "
            f"{orphans['fips_code'].head(5).tolist()}"
        )


def _check_orphan_zip_mappings(
    jurisdictions: pd.DataFrame,
    zips: pd.DataFrame,
    result: ValidationResult,
) -> None:
    """Warn if ZIP mappings reference unknown FIPS codes."""
    if zips.empty or jurisdictions.empty:
        return

    known_fips = set(jurisdictions["fips_code"])
    orphan_mask = ~zips["fips_code"].isin(known_fips)
    orphans = zips[orphan_mask]

    if not orphans.empty:
        result.warnings.append(
            f"{len(orphans)} ZIP mappings reference unknown jurisdictions: "
            f"{orphans['fips_code'].head(5).tolist()}"
        )


def _check_rate_deltas(rates: pd.DataFrame, result: ValidationResult) -> None:
    """Compare new rates against current production rates.

    Flag any jurisdiction where the rate changed by more than MAX_RATE_DELTA
    (likely a data error, not a real rate change).
    """
    if rates.empty:
        return

    try:
        conn = psycopg2.connect(DATABASE_URL)
    except psycopg2.OperationalError:
        result.warnings.append(
            "Could not connect to database for rate delta check — skipping."
        )
        return

    try:
        current = pd.read_sql(
            "SELECT fips_code, rate FROM rates WHERE expiry_date IS NULL AND rate_type = 'general'",
            conn,
        )
    except Exception as exc:
        result.warnings.append(f"Could not read current rates for delta check: {exc}")
        return
    finally:
        conn.close()

    if current.empty:
        logger.info("No existing rates in production — delta check skipped (first load).")
        return

    merged = rates.merge(current, on="fips_code", suffixes=("_new", "_old"))
    merged["delta"] = abs(merged["rate_new"] - merged["rate_old"])
    big_changes = merged[merged["delta"] > MAX_RATE_DELTA]

    if not big_changes.empty:
        samples = big_changes[["fips_code", "rate_old", "rate_new", "delta"]].head(10)
        result.warnings.append(
            f"{len(big_changes)} jurisdictions have rate changes > {MAX_RATE_DELTA:.1%}:\n"
            f"{samples.to_string(index=False)}"
        )
