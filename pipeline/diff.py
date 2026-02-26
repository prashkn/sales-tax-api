"""Compare staging data against current production and generate a diff report.

The report is a human-readable text file showing:
  - New jurisdictions added
  - Jurisdictions removed
  - Rate increases and decreases
  - New ZIP mappings
  - Removed ZIP mappings
  - Summary statistics

This report is reviewed by a human before promoting staging → production.
"""

import logging
from datetime import datetime
from io import StringIO
from pathlib import Path

import pandas as pd
import psycopg2

from config import DATABASE_URL, REPORTS_DIR, current_quarter

logger = logging.getLogger(__name__)


def generate_diff_report(
    new_jurisdictions: pd.DataFrame,
    new_rates: pd.DataFrame,
    new_zips: pd.DataFrame,
) -> str:
    """Generate a diff report comparing new data to current production.

    Returns the report as a string and also writes it to a file.
    """
    buf = StringIO()
    _write = lambda s="": buf.write(s + "\n")

    _write(f"{'=' * 72}")
    _write(f"  SALES TAX DATA PIPELINE — DIFF REPORT")
    _write(f"  Quarter: {current_quarter()}")
    _write(f"  Generated: {datetime.now().isoformat(timespec='seconds')}")
    _write(f"{'=' * 72}")
    _write()

    # Try to load current production data.
    try:
        conn = psycopg2.connect(DATABASE_URL)
    except Exception as exc:
        _write(f"[!] Could not connect to production database: {exc}")
        _write("    Cannot generate diff — showing new data summary only.")
        _write()
        _write_new_data_summary(buf, new_jurisdictions, new_rates, new_zips)
        return _finalize(buf)

    try:
        cur_jurisdictions = pd.read_sql("SELECT * FROM jurisdictions", conn)
        cur_rates = pd.read_sql(
            "SELECT fips_code, rate, rate_type, source FROM rates "
            "WHERE expiry_date IS NULL AND rate_type = 'general'",
            conn,
        )
        cur_zips = pd.read_sql(
            "SELECT zip_code, fips_code FROM zip_to_jurisdictions WHERE expiry_date IS NULL",
            conn,
        )
    except Exception as exc:
        _write(f"[!] Could not read production data: {exc}")
        _write("    Cannot generate diff — showing new data summary only.")
        _write()
        _write_new_data_summary(buf, new_jurisdictions, new_rates, new_zips)
        return _finalize(buf)
    finally:
        conn.close()

    # --- Jurisdictions diff ---
    _write("JURISDICTIONS")
    _write("-" * 72)

    cur_fips = set(cur_jurisdictions["fips_code"])
    new_fips = set(new_jurisdictions["fips_code"]) if not new_jurisdictions.empty else set()

    added_fips = new_fips - cur_fips
    removed_fips = cur_fips - new_fips

    _write(f"  Current:  {len(cur_fips):>8,}")
    _write(f"  New:      {len(new_fips):>8,}")
    _write(f"  Added:    {len(added_fips):>8,}")
    _write(f"  Removed:  {len(removed_fips):>8,}")
    _write()

    if added_fips:
        _write("  Added jurisdictions (first 20):")
        added_rows = new_jurisdictions[new_jurisdictions["fips_code"].isin(added_fips)].head(20)
        for _, row in added_rows.iterrows():
            _write(f"    + {row['fips_code']:20s}  {row.get('name', 'N/A'):30s}  {row.get('type', 'N/A')}")
        if len(added_fips) > 20:
            _write(f"    ... and {len(added_fips) - 20} more")
        _write()

    if removed_fips:
        _write("  Removed jurisdictions (first 20):")
        removed_rows = cur_jurisdictions[cur_jurisdictions["fips_code"].isin(removed_fips)].head(20)
        for _, row in removed_rows.iterrows():
            _write(f"    - {row['fips_code']:20s}  {row.get('name', 'N/A'):30s}  {row.get('type', 'N/A')}")
        if len(removed_fips) > 20:
            _write(f"    ... and {len(removed_fips) - 20} more")
        _write()

    # --- Rate changes ---
    _write("RATE CHANGES")
    _write("-" * 72)

    if not new_rates.empty and not cur_rates.empty:
        merged = new_rates.merge(cur_rates, on="fips_code", suffixes=("_new", "_old"))
        merged["delta"] = merged["rate_new"] - merged["rate_old"]
        changed = merged[merged["delta"].abs() > 1e-6]

        increases = changed[changed["delta"] > 0]
        decreases = changed[changed["delta"] < 0]

        _write(f"  Rates compared:  {len(merged):>8,}")
        _write(f"  Increased:       {len(increases):>8,}")
        _write(f"  Decreased:       {len(decreases):>8,}")
        _write(f"  Unchanged:       {len(merged) - len(changed):>8,}")
        _write()

        if not increases.empty:
            _write("  Rate increases (first 20):")
            for _, row in increases.head(20).iterrows():
                _write(
                    f"    ↑ {row['fips_code']:20s}  "
                    f"{row['rate_old']:.5f} → {row['rate_new']:.5f}  "
                    f"(+{row['delta']:.5f})"
                )
            if len(increases) > 20:
                _write(f"    ... and {len(increases) - 20} more")
            _write()

        if not decreases.empty:
            _write("  Rate decreases (first 20):")
            for _, row in decreases.head(20).iterrows():
                _write(
                    f"    ↓ {row['fips_code']:20s}  "
                    f"{row['rate_old']:.5f} → {row['rate_new']:.5f}  "
                    f"({row['delta']:.5f})"
                )
            if len(decreases) > 20:
                _write(f"    ... and {len(decreases) - 20} more")
            _write()

        # New rates (FIPS codes not in production)
        new_rate_fips = set(new_rates["fips_code"]) - set(cur_rates["fips_code"])
        if new_rate_fips:
            _write(f"  New rates (no previous entry): {len(new_rate_fips):,}")
            new_only = new_rates[new_rates["fips_code"].isin(new_rate_fips)].head(10)
            for _, row in new_only.iterrows():
                _write(f"    + {row['fips_code']:20s}  {row['rate']:.5f}")
            if len(new_rate_fips) > 10:
                _write(f"    ... and {len(new_rate_fips) - 10} more")
            _write()
    else:
        _write("  No rate comparison possible (empty dataset).")
        _write()

    # --- ZIP mapping changes ---
    _write("ZIP CODE MAPPINGS")
    _write("-" * 72)

    if not new_zips.empty and not cur_zips.empty:
        cur_pairs = set(zip(cur_zips["zip_code"], cur_zips["fips_code"]))
        new_pairs = set(zip(new_zips["zip_code"], new_zips["fips_code"]))

        added_pairs = new_pairs - cur_pairs
        removed_pairs = cur_pairs - new_pairs

        _write(f"  Current mappings: {len(cur_pairs):>8,}")
        _write(f"  New mappings:     {len(new_pairs):>8,}")
        _write(f"  Added:            {len(added_pairs):>8,}")
        _write(f"  Removed:          {len(removed_pairs):>8,}")
    else:
        _write("  No ZIP mapping comparison possible (empty dataset).")
    _write()

    # --- Summary ---
    _write("=" * 72)
    _write("  SUMMARY")
    _write(f"  Jurisdictions: {len(new_fips):,} total, {len(added_fips):,} new, {len(removed_fips):,} removed")
    if not new_rates.empty:
        _write(f"  Rates: {len(new_rates):,} total")
    if not new_zips.empty:
        _write(f"  ZIP mappings: {len(new_zips):,} total")
    _write("=" * 72)

    return _finalize(buf)


def _write_new_data_summary(
    buf: StringIO,
    jurisdictions: pd.DataFrame,
    rates: pd.DataFrame,
    zips: pd.DataFrame,
) -> None:
    """Write a summary when no production data is available for comparison."""
    _write = lambda s="": buf.write(s + "\n")

    _write("NEW DATA SUMMARY (no production baseline)")
    _write("-" * 72)
    _write(f"  Jurisdictions: {len(jurisdictions):,}")
    _write(f"  Rates:         {len(rates):,}")
    _write(f"  ZIP mappings:  {len(zips):,}")

    if not jurisdictions.empty:
        _write()
        _write("  Jurisdiction types:")
        for jtype, count in jurisdictions["type"].value_counts().items():
            _write(f"    {jtype:20s}: {count:,}")

    if not rates.empty:
        _write()
        _write(f"  Rate range: {rates['rate'].min():.5f} – {rates['rate'].max():.5f}")
        _write(f"  Mean rate:  {rates['rate'].mean():.5f}")

    if not zips.empty:
        _write()
        _write(f"  Unique ZIPs covered: {zips['zip_code'].nunique():,}")


def _finalize(buf: StringIO) -> str:
    """Write the report to a file and return the content."""
    report = buf.getvalue()

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    report_path = REPORTS_DIR / f"diff_{current_quarter()}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    report_path.write_text(report)
    logger.info("Diff report written to %s", report_path)

    return report
