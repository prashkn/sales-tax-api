"""Parse and normalize heterogeneous CSV formats into a unified schema.

Each data source has its own parser that returns three DataFrames aligned to
the staging table schemas: jurisdictions, rates, zip_to_jurisdictions.
"""

import logging
from datetime import date
from pathlib import Path

import pandas as pd

from config import SST_STATES, current_quarter

logger = logging.getLogger(__name__)

# Effective date used for newly ingested records this quarter.
_EFFECTIVE_DATE = date.today().replace(day=1)


# ---------------------------------------------------------------------------
# Output schema definitions (column names match staging tables)
# ---------------------------------------------------------------------------

JURISDICTIONS_COLS = [
    "fips_code", "name", "type", "state_fips", "parent_fips", "effective_date",
]

RATES_COLS = [
    "fips_code", "rate", "rate_type", "effective_date", "expiry_date", "source",
]

ZIP_JUNCTIONS_COLS = [
    "zip_code", "fips_code", "is_primary", "effective_date", "expiry_date",
]


def _empty_jurisdictions() -> pd.DataFrame:
    return pd.DataFrame(columns=JURISDICTIONS_COLS)


def _empty_rates() -> pd.DataFrame:
    return pd.DataFrame(columns=RATES_COLS)


def _empty_zip_junctions() -> pd.DataFrame:
    return pd.DataFrame(columns=ZIP_JUNCTIONS_COLS)


# ---------------------------------------------------------------------------
# SST parsers
# ---------------------------------------------------------------------------

def _parse_sst_rate_file(path: Path, state_abbr: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Parse an SST per-state rate CSV into jurisdictions + rates DataFrames.

    Typical SST rate file columns:
      State, JurisdictionFIPS, JurisdictionName, JurisdictionType,
      GeneralRateState, GeneralRateCounty, GeneralRateCity, GeneralRateSpecial
    """
    try:
        df = pd.read_csv(path, dtype=str, encoding="utf-8-sig")
    except Exception:
        logger.warning("Could not read SST rate file %s, skipping", path)
        return _empty_jurisdictions(), _empty_rates()

    # Normalize column names (strip whitespace, lowercase).
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    # Map expected column names (SST files vary slightly between states).
    fips_col = _find_col(df, ["jurisdictionfips", "fips", "jurisdiction_fips_code"])
    name_col = _find_col(df, ["jurisdictionname", "jurisdiction_name", "name"])
    type_col = _find_col(df, ["jurisdictiontype", "jurisdiction_type", "type"])

    if not all([fips_col, name_col, type_col]):
        logger.warning("SST rate file %s missing required columns, skipping", path)
        return _empty_jurisdictions(), _empty_rates()

    state_fips = SST_STATES.get(state_abbr, "")

    # --- Jurisdictions ---
    jurisdictions = pd.DataFrame({
        "fips_code": df[fips_col].str.strip(),
        "name": df[name_col].str.strip(),
        "type": df[type_col].str.strip().str.lower().replace({
            "state": "state", "county": "county", "city": "city",
            "special": "special_district", "special_district": "special_district",
        }),
        "state_fips": state_fips,
        "parent_fips": None,
        "effective_date": _EFFECTIVE_DATE,
    })

    # --- Rates ---
    # Look for general rate columns. SST files typically split by jurisdiction level,
    # but we want one rate per FIPS code (the jurisdiction's own rate contribution).
    rate_col = _find_col(df, [
        "generalrateintrastate", "general_rate_intrastate",
        "generalrate", "general_rate", "rate",
    ])

    if rate_col:
        rates = pd.DataFrame({
            "fips_code": df[fips_col].str.strip(),
            "rate": pd.to_numeric(df[rate_col], errors="coerce").fillna(0),
            "rate_type": "general",
            "effective_date": _EFFECTIVE_DATE,
            "expiry_date": None,
            "source": "sst",
        })
    else:
        rates = _empty_rates()

    jurisdictions = jurisdictions.drop_duplicates(subset=["fips_code"])
    rates = rates.drop_duplicates(subset=["fips_code"])
    return jurisdictions, rates


def _parse_sst_boundary_file(path: Path, state_abbr: str) -> pd.DataFrame:
    """Parse an SST boundary CSV into a zip_to_jurisdictions DataFrame.

    Typical columns:
      RecordType, ZipCode, State, FIPSStateCode, FIPSCountyCode,
      FIPSPlaceCode, ...
    """
    try:
        df = pd.read_csv(path, dtype=str, encoding="utf-8-sig")
    except Exception:
        logger.warning("Could not read SST boundary file %s, skipping", path)
        return _empty_zip_junctions()

    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    zip_col = _find_col(df, ["zipcode", "zip_code", "zip"])
    fips_col = _find_col(df, [
        "compositefips", "composite_fips", "fips",
        "jurisdictionfips", "jurisdiction_fips",
    ])

    if not all([zip_col, fips_col]):
        logger.warning("SST boundary file %s missing required columns, skipping", path)
        return _empty_zip_junctions()

    result = pd.DataFrame({
        "zip_code": df[zip_col].str.strip().str[:5],  # Use 5-digit ZIP
        "fips_code": df[fips_col].str.strip(),
        "is_primary": True,
        "effective_date": _EFFECTIVE_DATE,
        "expiry_date": None,
    })

    return result.drop_duplicates(subset=["zip_code", "fips_code"])


def parse_sst(files: list[Path]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Parse all downloaded SST files.

    Returns (jurisdictions, rates, zip_to_jurisdictions) DataFrames.
    """
    all_jurisdictions = []
    all_rates = []
    all_zips = []

    for path in files:
        # Extract state abbreviation from filename like "WA_rates.csv"
        stem = path.stem.upper()
        state_abbr = stem.split("_")[0]

        if "_rates" in path.stem.lower() or "_rate" in path.stem.lower():
            j, r = _parse_sst_rate_file(path, state_abbr)
            all_jurisdictions.append(j)
            all_rates.append(r)
        elif "_boundar" in path.stem.lower():
            z = _parse_sst_boundary_file(path, state_abbr)
            all_zips.append(z)

    return (
        pd.concat(all_jurisdictions, ignore_index=True) if all_jurisdictions else _empty_jurisdictions(),
        pd.concat(all_rates, ignore_index=True) if all_rates else _empty_rates(),
        pd.concat(all_zips, ignore_index=True) if all_zips else _empty_zip_junctions(),
    )


# ---------------------------------------------------------------------------
# Avalara parser
# ---------------------------------------------------------------------------

def parse_avalara(files: list[Path]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Parse Avalara's all-states tax rate CSV.

    Typical Avalara columns:
      State, ZipCode, TaxRegionName, TaxRegionCode,
      EstimatedCombinedRate, EstimatedStateRate, EstimatedCountyRate,
      EstimatedCityRate, EstimatedSpecialRate
    """
    if not files:
        return _empty_jurisdictions(), _empty_rates(), _empty_zip_junctions()

    all_jurisdictions = []
    all_rates = []
    all_zips = []

    for path in files:
        try:
            df = pd.read_csv(path, dtype=str, encoding="utf-8-sig")
        except Exception:
            logger.warning("Could not read Avalara file %s, skipping", path)
            continue

        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

        zip_col = _find_col(df, ["zipcode", "zip_code", "zip"])
        state_col = _find_col(df, ["state", "state_abbreviation"])
        name_col = _find_col(df, ["taxregionname", "tax_region_name", "jurisdiction"])
        code_col = _find_col(df, ["taxregioncode", "tax_region_code"])

        # Rate columns
        state_rate_col = _find_col(df, ["estimatedstaterate", "estimated_state_rate", "state_rate"])
        county_rate_col = _find_col(df, ["estimatedcountyrate", "estimated_county_rate", "county_rate"])
        city_rate_col = _find_col(df, ["estimatedcityrate", "estimated_city_rate", "city_rate"])
        special_rate_col = _find_col(df, ["estimatedspecialrate", "estimated_special_rate", "special_rate"])

        if not zip_col:
            logger.warning("Avalara file %s missing ZIP column, skipping", path)
            continue

        # State FIPS lookup table
        state_abbr_to_fips = {v: k for k, v in _STATE_FIPS.items()}

        for _, row in df.iterrows():
            zip_code = str(row.get(zip_col, "")).strip()[:5]
            if not zip_code or len(zip_code) != 5:
                continue

            state_abbr = str(row.get(state_col, "")).strip() if state_col else ""
            state_fips = state_abbr_to_fips.get(state_abbr, "")
            region_name = str(row.get(name_col, "")).strip() if name_col else ""
            region_code = str(row.get(code_col, "")).strip() if code_col else ""

            # Use the region code as a FIPS-like identifier, or construct one.
            fips = region_code if region_code else f"{state_fips}Z{zip_code}"

            # Build a state-level jurisdiction (deduplicated later).
            if state_fips:
                all_jurisdictions.append({
                    "fips_code": state_fips,
                    "name": state_abbr,
                    "type": "state",
                    "state_fips": state_fips,
                    "parent_fips": None,
                    "effective_date": _EFFECTIVE_DATE,
                })

                if state_rate_col:
                    rate_val = _safe_float(row.get(state_rate_col))
                    all_rates.append({
                        "fips_code": state_fips,
                        "rate": rate_val,
                        "rate_type": "general",
                        "effective_date": _EFFECTIVE_DATE,
                        "expiry_date": None,
                        "source": "avalara",
                    })

                all_zips.append({
                    "zip_code": zip_code,
                    "fips_code": state_fips,
                    "is_primary": True,
                    "effective_date": _EFFECTIVE_DATE,
                    "expiry_date": None,
                })

            # County, city, special — build combined jurisdiction rows.
            for rate_col_name, jur_type, suffix in [
                (county_rate_col, "county", "C"),
                (city_rate_col, "city", "I"),
                (special_rate_col, "special_district", "S"),
            ]:
                if not rate_col_name:
                    continue
                rate_val = _safe_float(row.get(rate_col_name))
                if rate_val <= 0:
                    continue

                sub_fips = f"{state_fips}{suffix}{zip_code}"
                all_jurisdictions.append({
                    "fips_code": sub_fips,
                    "name": f"{region_name} ({jur_type})",
                    "type": jur_type,
                    "state_fips": state_fips,
                    "parent_fips": state_fips,
                    "effective_date": _EFFECTIVE_DATE,
                })
                all_rates.append({
                    "fips_code": sub_fips,
                    "rate": rate_val,
                    "rate_type": "general",
                    "effective_date": _EFFECTIVE_DATE,
                    "expiry_date": None,
                    "source": "avalara",
                })
                all_zips.append({
                    "zip_code": zip_code,
                    "fips_code": sub_fips,
                    "is_primary": True,
                    "effective_date": _EFFECTIVE_DATE,
                    "expiry_date": None,
                })

    jurisdictions_df = pd.DataFrame(all_jurisdictions) if all_jurisdictions else _empty_jurisdictions()
    rates_df = pd.DataFrame(all_rates) if all_rates else _empty_rates()
    zips_df = pd.DataFrame(all_zips) if all_zips else _empty_zip_junctions()

    # Deduplicate — keep first occurrence per FIPS code.
    jurisdictions_df = jurisdictions_df.drop_duplicates(subset=["fips_code"], keep="first")
    rates_df = rates_df.drop_duplicates(subset=["fips_code"], keep="first")
    zips_df = zips_df.drop_duplicates(subset=["zip_code", "fips_code"], keep="first")

    return jurisdictions_df, rates_df, zips_df


# ---------------------------------------------------------------------------
# Merge across sources
# ---------------------------------------------------------------------------

def merge_sources(
    sources: dict[str, tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]],
    priority: dict[str, int],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Merge parsed data from multiple sources, respecting source priority.

    When the same fips_code appears in multiple sources, keep the record
    from the source with the highest priority number.
    """
    all_j, all_r, all_z = [], [], []

    for source_name, (j, r, z) in sources.items():
        if not j.empty:
            j = j.copy()
            j["_priority"] = priority.get(source_name, 0)
            all_j.append(j)
        if not r.empty:
            r = r.copy()
            r["_priority"] = priority.get(source_name, 0)
            all_r.append(r)
        if not z.empty:
            all_z.append(z)

    # Jurisdictions: highest priority per fips_code wins.
    if all_j:
        merged_j = pd.concat(all_j, ignore_index=True)
        merged_j = merged_j.sort_values("_priority", ascending=False)
        merged_j = merged_j.drop_duplicates(subset=["fips_code"], keep="first")
        merged_j = merged_j.drop(columns=["_priority"])
    else:
        merged_j = _empty_jurisdictions()

    # Rates: highest priority per fips_code wins.
    if all_r:
        merged_r = pd.concat(all_r, ignore_index=True)
        merged_r = merged_r.sort_values("_priority", ascending=False)
        merged_r = merged_r.drop_duplicates(subset=["fips_code"], keep="first")
        merged_r = merged_r.drop(columns=["_priority"])
    else:
        merged_r = _empty_rates()

    # ZIP mappings: union of all sources (a ZIP can map to many FIPS codes).
    if all_z:
        merged_z = pd.concat(all_z, ignore_index=True)
        merged_z = merged_z.drop_duplicates(subset=["zip_code", "fips_code"], keep="first")
    else:
        merged_z = _empty_zip_junctions()

    logger.info(
        "Merged: %d jurisdictions, %d rates, %d ZIP mappings",
        len(merged_j), len(merged_r), len(merged_z),
    )
    return merged_j, merged_r, merged_z


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _find_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """Find the first matching column name in a DataFrame (case-insensitive)."""
    lower_cols = {c.lower().replace(" ", "_"): c for c in df.columns}
    for candidate in candidates:
        if candidate.lower() in lower_cols:
            return lower_cols[candidate.lower()]
    return None


def _safe_float(val) -> float:
    """Safely convert a value to float, returning 0.0 on failure."""
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0


# State abbreviation → FIPS code mapping.
_STATE_FIPS: dict[str, str] = {
    "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA",
    "08": "CO", "09": "CT", "10": "DE", "11": "DC", "12": "FL",
    "13": "GA", "15": "HI", "16": "ID", "17": "IL", "18": "IN",
    "19": "IA", "20": "KS", "21": "KY", "22": "LA", "23": "ME",
    "24": "MD", "25": "MA", "26": "MI", "27": "MN", "28": "MS",
    "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH",
    "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND",
    "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI",
    "45": "SC", "46": "SD", "47": "TN", "48": "TX", "49": "UT",
    "50": "VT", "51": "VA", "53": "WA", "54": "WV", "55": "WI",
    "56": "WY",
}
