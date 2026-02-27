"""Write validated data to staging tables and promote staging → production.

Two distinct operations:
  1. load_staging() — Truncate staging tables and insert new data.
  2. promote()      — Merge staging into production with upsert logic,
                      recording all rate changes in rate_history.

The pipeline NEVER writes directly to production tables.
"""

from __future__ import annotations

import logging
from datetime import date

import psycopg2
from psycopg2.extras import execute_values

from config import DATABASE_URL, pipeline_run_id

logger = logging.getLogger(__name__)


def load_staging(
    jurisdictions: pd.DataFrame,
    rates: pd.DataFrame,
    zip_to_jurisdictions: pd.DataFrame,
) -> None:
    """Truncate staging tables and insert the new data.

    This is safe to run multiple times — it always starts fresh.
    """
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            # Truncate staging tables.
            cur.execute("TRUNCATE TABLE zip_to_jurisdictions_staging")
            cur.execute("TRUNCATE TABLE rates_staging")
            cur.execute("TRUNCATE TABLE jurisdictions_staging")
            logger.info("Truncated staging tables")

            # Insert jurisdictions.
            if not jurisdictions.empty:
                _insert_jurisdictions_staging(cur, jurisdictions)

            # Insert rates.
            if not rates.empty:
                _insert_rates_staging(cur, rates)

            # Insert ZIP mappings.
            if not zip_to_jurisdictions.empty:
                _insert_zip_staging(cur, zip_to_jurisdictions)

        conn.commit()
        logger.info("Staging load complete")

    except Exception:
        conn.rollback()
        logger.exception("Staging load failed — rolled back")
        raise
    finally:
        conn.close()


def promote() -> dict:
    """Promote staging data to production tables.

    Uses an upsert strategy:
      - Jurisdictions: INSERT ON CONFLICT UPDATE
      - Rates: Expire old rates (set expiry_date), insert new ones
      - ZIP mappings: Expire old mappings, insert new ones
      - Rate changes are recorded in rate_history

    Returns a summary dict with counts.
    """
    run_id = pipeline_run_id()
    today = date.today()
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False

    summary = {
        "jurisdictions_upserted": 0,
        "rates_expired": 0,
        "rates_inserted": 0,
        "zip_mappings_expired": 0,
        "zip_mappings_inserted": 0,
        "rate_history_entries": 0,
    }

    try:
        with conn.cursor() as cur:
            # --- Jurisdictions: upsert from staging ---
            cur.execute("""
                INSERT INTO jurisdictions (fips_code, name, type, state_fips, parent_fips, effective_date, updated_at)
                SELECT fips_code, name, type, state_fips, parent_fips, effective_date, now()
                FROM jurisdictions_staging
                ON CONFLICT (fips_code) DO UPDATE SET
                    name = EXCLUDED.name,
                    type = EXCLUDED.type,
                    state_fips = EXCLUDED.state_fips,
                    parent_fips = EXCLUDED.parent_fips,
                    effective_date = EXCLUDED.effective_date,
                    updated_at = now()
            """)
            summary["jurisdictions_upserted"] = cur.rowcount
            logger.info("Upserted %d jurisdictions", cur.rowcount)

            # --- Rates: record changes, expire old, insert new ---

            # Find rate changes and record in rate_history.
            cur.execute("""
                INSERT INTO rate_history (fips_code, old_rate, new_rate, changed_date, source, pipeline_run_id)
                SELECT
                    s.fips_code,
                    p.rate AS old_rate,
                    s.rate AS new_rate,
                    %s AS changed_date,
                    s.source,
                    %s AS pipeline_run_id
                FROM rates_staging s
                JOIN rates p ON p.fips_code = s.fips_code
                    AND p.expiry_date IS NULL
                    AND p.rate_type = 'general'
                WHERE s.rate_type = 'general'
                    AND s.rate != p.rate
            """, (today, run_id))
            summary["rate_history_entries"] = cur.rowcount
            logger.info("Recorded %d rate changes in history", cur.rowcount)

            # Expire current active rates that have a corresponding staging record.
            cur.execute("""
                UPDATE rates
                SET expiry_date = %s
                WHERE expiry_date IS NULL
                    AND rate_type = 'general'
                    AND fips_code IN (SELECT fips_code FROM rates_staging WHERE rate_type = 'general')
            """, (today,))
            summary["rates_expired"] = cur.rowcount
            logger.info("Expired %d old rates", cur.rowcount)

            # Insert new rates from staging.
            cur.execute("""
                INSERT INTO rates (fips_code, rate, rate_type, effective_date, expiry_date, source)
                SELECT fips_code, rate, rate_type, effective_date, expiry_date, source
                FROM rates_staging
            """)
            summary["rates_inserted"] = cur.rowcount
            logger.info("Inserted %d new rates", cur.rowcount)

            # --- ZIP mappings: expire old, insert new ---

            # Expire current mappings that have a corresponding staging record.
            cur.execute("""
                UPDATE zip_to_jurisdictions
                SET expiry_date = %s
                WHERE expiry_date IS NULL
                    AND (zip_code, fips_code) IN (
                        SELECT zip_code, fips_code FROM zip_to_jurisdictions_staging
                    )
            """, (today,))
            summary["zip_mappings_expired"] = cur.rowcount
            logger.info("Expired %d old ZIP mappings", cur.rowcount)

            # Insert new ZIP mappings from staging.
            cur.execute("""
                INSERT INTO zip_to_jurisdictions (zip_code, fips_code, is_primary, effective_date, expiry_date)
                SELECT zip_code, fips_code, is_primary, effective_date, expiry_date
                FROM zip_to_jurisdictions_staging
                ON CONFLICT (zip_code, fips_code, effective_date) DO UPDATE SET
                    is_primary = EXCLUDED.is_primary,
                    expiry_date = EXCLUDED.expiry_date
            """)
            summary["zip_mappings_inserted"] = cur.rowcount
            logger.info("Inserted %d new ZIP mappings", cur.rowcount)

        conn.commit()
        logger.info("Promotion complete: %s", summary)

    except Exception:
        conn.rollback()
        logger.exception("Promotion failed — rolled back")
        raise
    finally:
        conn.close()

    return summary


# ---------------------------------------------------------------------------
# Staging insert helpers
# ---------------------------------------------------------------------------

def _insert_jurisdictions_staging(cur, df) -> None:
    """Bulk insert jurisdictions into staging."""
    values = [
        (
            row["fips_code"],
            row["name"],
            row["type"],
            row["state_fips"],
            row.get("parent_fips"),
            row["effective_date"],
        )
        for _, row in df.iterrows()
    ]
    execute_values(
        cur,
        """
        INSERT INTO jurisdictions_staging (fips_code, name, type, state_fips, parent_fips, effective_date)
        VALUES %s
        ON CONFLICT (fips_code) DO UPDATE SET
            name = EXCLUDED.name,
            type = EXCLUDED.type,
            state_fips = EXCLUDED.state_fips,
            parent_fips = EXCLUDED.parent_fips,
            effective_date = EXCLUDED.effective_date
        """,
        values,
        page_size=1000,
    )
    logger.info("Inserted %d jurisdictions into staging", len(values))


def _insert_rates_staging(cur, df) -> None:
    """Bulk insert rates into staging."""
    values = [
        (
            row["fips_code"],
            float(row["rate"]),
            row.get("rate_type", "general"),
            row["effective_date"],
            row.get("expiry_date"),
            row.get("source", "unknown"),
        )
        for _, row in df.iterrows()
    ]
    execute_values(
        cur,
        """
        INSERT INTO rates_staging (fips_code, rate, rate_type, effective_date, expiry_date, source)
        VALUES %s
        """,
        values,
        page_size=1000,
    )
    logger.info("Inserted %d rates into staging", len(values))


def _insert_zip_staging(cur, df) -> None:
    """Bulk insert ZIP-to-jurisdiction mappings into staging."""
    values = [
        (
            row["zip_code"],
            row["fips_code"],
            bool(row.get("is_primary", True)),
            row["effective_date"],
            row.get("expiry_date"),
        )
        for _, row in df.iterrows()
    ]
    execute_values(
        cur,
        """
        INSERT INTO zip_to_jurisdictions_staging (zip_code, fips_code, is_primary, effective_date, expiry_date)
        VALUES %s
        ON CONFLICT (zip_code, fips_code, effective_date) DO UPDATE SET
            is_primary = EXCLUDED.is_primary,
            expiry_date = EXCLUDED.expiry_date
        """,
        values,
        page_size=1000,
    )
    logger.info("Inserted %d ZIP mappings into staging", len(values))
