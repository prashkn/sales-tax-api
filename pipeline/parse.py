"""Parse and normalize heterogeneous CSV formats into a unified schema.

Each data source has its own parser that returns three DataFrames aligned to
the staging table schemas: jurisdictions, rates, zip_to_jurisdictions.
"""

import logging
from datetime import date
from pathlib import Path

import pandas as pd

from config import SST_STATES, current_quarter

# Set of state FIPS codes covered by SST (used to avoid Avalara overlap).
_SST_STATE_FIPS = set(SST_STATES.values())

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

    Approach:
      - State-level jurisdictions use real state FIPS codes.
      - Sub-state rates (county + city + special) are combined into a single
        synthetic "local" jurisdiction per ZIP, since Avalara doesn't provide
        real FIPS codes for sub-state jurisdictions.  The synthetic FIPS format
        is ``AVL{state_fips}{zip}`` (e.g., ``AVL0690210``), clearly marking
        them as Avalara-sourced so they don't collide with real FIPS codes
        from SST.
      - Uses vectorized pandas operations instead of row-by-row iteration.
    """
    if not files:
        return _empty_jurisdictions(), _empty_rates(), _empty_zip_junctions()

    # Build lookup: state abbreviation → FIPS code (computed once).
    state_abbr_to_fips = _STATE_TO_FIPS

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

        # Rate columns
        state_rate_col = _find_col(df, ["estimatedstaterate", "estimated_state_rate", "state_rate"])
        county_rate_col = _find_col(df, ["estimatedcountyrate", "estimated_county_rate", "county_rate"])
        city_rate_col = _find_col(df, ["estimatedcityrate", "estimated_city_rate", "city_rate"])
        special_rate_col = _find_col(df, ["estimatedspecialrate", "estimated_special_rate", "special_rate"])

        if not zip_col:
            logger.warning("Avalara file %s missing ZIP column, skipping", path)
            continue

        # --- Vectorized pre-processing ---
        df["_zip"] = df[zip_col].str.strip().str[:5]
        df = df[df["_zip"].str.len() == 5].copy()

        if state_col:
            df["_state_abbr"] = df[state_col].str.strip()
            df["_state_fips"] = df["_state_abbr"].map(state_abbr_to_fips).fillna("")
        else:
            df["_state_abbr"] = ""
            df["_state_fips"] = ""

        if name_col:
            df["_region_name"] = df[name_col].str.strip()
        else:
            df["_region_name"] = ""

        # --- State-level jurisdictions (real FIPS, deduplicated later) ---
        states_with_fips = df[df["_state_fips"] != ""].drop_duplicates(subset=["_state_fips"])
        if not states_with_fips.empty:
            state_j = pd.DataFrame({
                "fips_code": states_with_fips["_state_fips"].values,
                "name": states_with_fips["_state_abbr"].values,
                "type": "state",
                "state_fips": states_with_fips["_state_fips"].values,
                "parent_fips": None,
                "effective_date": _EFFECTIVE_DATE,
            })
            all_jurisdictions.append(state_j)

            if state_rate_col:
                state_rates_df = states_with_fips.copy()
                state_rates_df["_rate"] = pd.to_numeric(state_rates_df[state_rate_col], errors="coerce").fillna(0)
                state_r = pd.DataFrame({
                    "fips_code": state_rates_df["_state_fips"].values,
                    "rate": state_rates_df["_rate"].values,
                    "rate_type": "general",
                    "effective_date": _EFFECTIVE_DATE,
                    "expiry_date": None,
                    "source": "avalara",
                })
                all_rates.append(state_r)

        # --- ZIP → state mappings ---
        zip_state = df[df["_state_fips"] != ""][["_zip", "_state_fips"]].drop_duplicates()
        if not zip_state.empty:
            state_z = pd.DataFrame({
                "zip_code": zip_state["_zip"].values,
                "fips_code": zip_state["_state_fips"].values,
                "is_primary": True,
                "effective_date": _EFFECTIVE_DATE,
                "expiry_date": None,
            })
            all_zips.append(state_z)

        # --- Local (sub-state) rates as a single combined jurisdiction per ZIP ---
        # Sum county + city + special into one "local" rate.  This avoids
        # synthesizing multiple fake FIPS codes that collide with real ones.
        local_rate_cols = [c for c in [county_rate_col, city_rate_col, special_rate_col] if c]
        if local_rate_cols:
            local = df[df["_state_fips"] != ""].copy()
            local["_local_rate"] = 0.0
            for col in local_rate_cols:
                local["_local_rate"] += pd.to_numeric(local[col], errors="coerce").fillna(0)

            # Only keep rows where the local rate is non-zero.
            local = local[local["_local_rate"] > 0]

            if not local.empty:
                # Deduplicate per ZIP (take first row per ZIP).
                local = local.drop_duplicates(subset=["_zip"])
                local["_local_fips"] = "AVL" + local["_state_fips"] + local["_zip"]

                local_j = pd.DataFrame({
                    "fips_code": local["_local_fips"].values,
                    "name": local["_region_name"].values + " (local)",
                    "type": "county",  # closest match for combined local rate
                    "state_fips": local["_state_fips"].values,
                    "parent_fips": local["_state_fips"].values,
                    "effective_date": _EFFECTIVE_DATE,
                })
                all_jurisdictions.append(local_j)

                local_r = pd.DataFrame({
                    "fips_code": local["_local_fips"].values,
                    "rate": local["_local_rate"].values,
                    "rate_type": "general",
                    "effective_date": _EFFECTIVE_DATE,
                    "expiry_date": None,
                    "source": "avalara",
                })
                all_rates.append(local_r)

                local_z = pd.DataFrame({
                    "zip_code": local["_zip"].values,
                    "fips_code": local["_local_fips"].values,
                    "is_primary": True,
                    "effective_date": _EFFECTIVE_DATE,
                    "expiry_date": None,
                })
                all_zips.append(local_z)

    jurisdictions_df = pd.concat(all_jurisdictions, ignore_index=True) if all_jurisdictions else _empty_jurisdictions()
    rates_df = pd.concat(all_rates, ignore_index=True) if all_rates else _empty_rates()
    zips_df = pd.concat(all_zips, ignore_index=True) if all_zips else _empty_zip_junctions()

    # Deduplicate.
    jurisdictions_df = jurisdictions_df.drop_duplicates(subset=["fips_code"], keep="first")
    rates_df = rates_df.drop_duplicates(subset=["fips_code"], keep="first")
    zips_df = zips_df.drop_duplicates(subset=["zip_code", "fips_code"], keep="first")

    return jurisdictions_df, rates_df, zips_df


# ---------------------------------------------------------------------------
# State government parser
# ---------------------------------------------------------------------------

def parse_state_gov(files: list[Path]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Parse state government CSV files.

    State gov CSVs vary widely in format.  This parser attempts the same
    column-detection heuristics as the Avalara parser.  Files that cannot
    be parsed are logged and skipped.

    The synthetic FIPS prefix for state_gov local jurisdictions is ``SGV``
    to distinguish them from Avalara's ``AVL`` prefix.
    """
    if not files:
        return _empty_jurisdictions(), _empty_rates(), _empty_zip_junctions()

    state_abbr_to_fips = _STATE_TO_FIPS

    all_jurisdictions: list[pd.DataFrame] = []
    all_rates: list[pd.DataFrame] = []
    all_zips: list[pd.DataFrame] = []

    for path in files:
        try:
            df = pd.read_csv(path, dtype=str, encoding="utf-8-sig")
        except Exception:
            logger.warning("Could not read state_gov file %s, skipping", path)
            continue

        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

        # Try to detect the state from the filename (e.g., NY_rates.csv).
        state_abbr = path.stem.upper().split("_")[0]

        zip_col = _find_col(df, ["zipcode", "zip_code", "zip"])
        state_col = _find_col(df, ["state", "state_abbreviation"])
        name_col = _find_col(df, ["jurisdiction", "taxregionname", "tax_region_name", "name"])

        rate_col = _find_col(df, [
            "rate", "combined_rate", "estimatedcombinedrate",
            "estimated_combined_rate", "total_rate", "salestaxrate",
        ])
        state_rate_col = _find_col(df, ["estimatedstaterate", "estimated_state_rate", "state_rate"])

        if not zip_col or not rate_col:
            logger.warning(
                "State gov file %s missing ZIP or rate column (found cols: %s), skipping",
                path, list(df.columns),
            )
            continue

        df["_zip"] = df[zip_col].str.strip().str[:5]
        df = df[df["_zip"].str.len() == 5].copy()

        if state_col:
            df["_state_fips"] = df[state_col].str.strip().map(state_abbr_to_fips).fillna("")
        else:
            df["_state_fips"] = state_abbr_to_fips.get(state_abbr, "")

        if name_col:
            df["_name"] = df[name_col].str.strip()
        else:
            df["_name"] = ""

        df["_rate"] = pd.to_numeric(df[rate_col], errors="coerce").fillna(0)

        # State rates.
        if state_rate_col:
            df["_state_rate"] = pd.to_numeric(df[state_rate_col], errors="coerce").fillna(0)
        else:
            df["_state_rate"] = 0.0

        # Compute local rate as combined minus state.
        df["_local_rate"] = (df["_rate"] - df["_state_rate"]).clip(lower=0)

        states_df = df[df["_state_fips"] != ""].drop_duplicates(subset=["_state_fips"])
        if not states_df.empty:
            sj = pd.DataFrame({
                "fips_code": states_df["_state_fips"].values,
                "name": state_abbr,
                "type": "state",
                "state_fips": states_df["_state_fips"].values,
                "parent_fips": None,
                "effective_date": _EFFECTIVE_DATE,
            })
            all_jurisdictions.append(sj)

            if state_rate_col:
                sr = pd.DataFrame({
                    "fips_code": states_df["_state_fips"].values,
                    "rate": states_df["_state_rate"].values,
                    "rate_type": "general",
                    "effective_date": _EFFECTIVE_DATE,
                    "expiry_date": None,
                    "source": "state_gov",
                })
                all_rates.append(sr)

        # ZIP → state mappings.
        zs = df[df["_state_fips"] != ""][["_zip", "_state_fips"]].drop_duplicates()
        if not zs.empty:
            all_zips.append(pd.DataFrame({
                "zip_code": zs["_zip"].values,
                "fips_code": zs["_state_fips"].values,
                "is_primary": True,
                "effective_date": _EFFECTIVE_DATE,
                "expiry_date": None,
            }))

        # Local rate jurisdictions.
        local = df[(df["_state_fips"] != "") & (df["_local_rate"] > 0)].copy()
        local = local.drop_duplicates(subset=["_zip"])
        if not local.empty:
            local["_local_fips"] = "SGV" + local["_state_fips"] + local["_zip"]

            all_jurisdictions.append(pd.DataFrame({
                "fips_code": local["_local_fips"].values,
                "name": local["_name"].values + " (local)",
                "type": "county",
                "state_fips": local["_state_fips"].values,
                "parent_fips": local["_state_fips"].values,
                "effective_date": _EFFECTIVE_DATE,
            }))
            all_rates.append(pd.DataFrame({
                "fips_code": local["_local_fips"].values,
                "rate": local["_local_rate"].values,
                "rate_type": "general",
                "effective_date": _EFFECTIVE_DATE,
                "expiry_date": None,
                "source": "state_gov",
            }))
            all_zips.append(pd.DataFrame({
                "zip_code": local["_zip"].values,
                "fips_code": local["_local_fips"].values,
                "is_primary": True,
                "effective_date": _EFFECTIVE_DATE,
                "expiry_date": None,
            }))

    jurisdictions_df = pd.concat(all_jurisdictions, ignore_index=True) if all_jurisdictions else _empty_jurisdictions()
    rates_df = pd.concat(all_rates, ignore_index=True) if all_rates else _empty_rates()
    zips_df = pd.concat(all_zips, ignore_index=True) if all_zips else _empty_zip_junctions()

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

    Additionally, for states covered by SST, Avalara's synthetic local
    jurisdictions (prefixed ``AVL``) are dropped entirely — SST provides
    real FIPS-level data for those states.
    """
    all_j, all_r, all_z = [], [], []

    # Determine which states have SST coverage so we can drop redundant
    # Avalara synthetic jurisdictions for those states.
    sst_covered_states = set()
    if "sst" in sources:
        sst_j = sources["sst"][0]
        if not sst_j.empty:
            sst_covered_states = set(sst_j["state_fips"].unique())
    if not sst_covered_states:
        sst_covered_states = _SST_STATE_FIPS

    for source_name, (j, r, z) in sources.items():
        # For Avalara, filter out synthetic local jurisdictions for SST states.
        if source_name == "avalara" and sst_covered_states:
            before_j = len(j)
            if not j.empty:
                j = j.copy()
                is_synthetic = j["fips_code"].str.startswith("AVL")
                is_sst_state = j["state_fips"].isin(sst_covered_states)
                j = j[~(is_synthetic & is_sst_state)]
            if not r.empty:
                r = r.copy()
                # Keep rates only for FIPS codes still in jurisdictions.
                kept_fips = set(j["fips_code"]) if not j.empty else set()
                r = r[r["fips_code"].isin(kept_fips) | ~r["fips_code"].str.startswith("AVL")]
            if not z.empty:
                z = z.copy()
                kept_fips_all = set(j["fips_code"]) if not j.empty else set()
                # Keep state-level ZIP mappings + non-AVL + AVL for non-SST states.
                z = z[z["fips_code"].isin(kept_fips_all) | ~z["fips_code"].str.startswith("AVL")]
            after_j = len(j)
            if before_j != after_j:
                logger.info(
                    "Filtered %d Avalara synthetic jurisdictions for SST-covered states",
                    before_j - after_j,
                )

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


# FIPS code → state abbreviation mapping.
_FIPS_TO_STATE: dict[str, str] = {
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

# Reverse lookup: state abbreviation → FIPS code.
_STATE_TO_FIPS: dict[str, str] = {v: k for k, v in _FIPS_TO_STATE.items()}
