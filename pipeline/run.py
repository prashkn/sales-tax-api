"""CLI entrypoint: orchestrates download → parse → validate → diff → load → promote.

Usage:
    python run.py --stage download
    python run.py --stage parse
    python run.py --stage validate
    python run.py --stage diff
    python run.py --stage load
    python run.py --stage promote --confirm
    python run.py --stage all          # run download through load (stops before promote)
    python run.py --stage all --confirm # run everything including promote
"""

import logging
import sys

import click

from config import SOURCE_PRIORITY, current_quarter, pipeline_run_id

# Module-level state shared between stages within a single run.
_state: dict = {}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("pipeline")


@click.command()
@click.option(
    "--stage",
    type=click.Choice(["download", "parse", "validate", "diff", "load", "promote", "all"]),
    required=True,
    help="Pipeline stage to run.",
)
@click.option(
    "--confirm",
    is_flag=True,
    default=False,
    help="Required for the promote stage to actually swap staging → production.",
)
def main(stage: str, confirm: bool) -> None:
    """Sales Tax Data Pipeline — quarterly data ingestion and refresh."""
    logger.info("Pipeline run: %s | stage=%s | quarter=%s", pipeline_run_id(), stage, current_quarter())

    run_all = stage == "all"

    if stage in ("download", "all"):
        _run_download()

    if stage in ("parse", "all"):
        _run_parse()

    if stage in ("validate", "all"):
        ok = _run_validate()
        if not ok and run_all:
            logger.error("Validation failed — stopping pipeline. Fix data issues before proceeding.")
            sys.exit(1)

    if stage in ("diff", "all"):
        _run_diff()

    if stage in ("load", "all"):
        _run_load()

    if stage in ("promote", "all"):
        _run_promote(confirm)

    logger.info("Pipeline stage '%s' complete.", stage)


# ---------------------------------------------------------------------------
# Stage runners
# ---------------------------------------------------------------------------

def _run_download() -> None:
    """Download all source CSVs."""
    logger.info("=" * 60)
    logger.info("STAGE: DOWNLOAD")
    logger.info("=" * 60)

    from download import download_all

    results = download_all()
    _state["downloaded_files"] = results

    total = sum(len(v) for v in results.values())
    logger.info("Downloaded %d files across %d sources", total, len(results))


def _run_parse() -> None:
    """Parse downloaded CSVs into unified DataFrames."""
    logger.info("=" * 60)
    logger.info("STAGE: PARSE")
    logger.info("=" * 60)

    from parse import merge_sources, parse_avalara, parse_sst, parse_state_gov

    downloaded = _state.get("downloaded_files")
    if not downloaded:
        logger.warning("No downloaded files in state — did you run the download stage first?")
        logger.info("Attempting to find raw files from previous run...")
        from config import RAW_DIR
        from pathlib import Path

        quarter_dir = RAW_DIR / current_quarter()
        if not quarter_dir.exists():
            logger.error("No raw files found for %s at %s", current_quarter(), quarter_dir)
            sys.exit(1)

        downloaded = {}
        sst_dir = quarter_dir / "sst"
        if sst_dir.exists():
            downloaded["sst"] = list(sst_dir.glob("*.csv"))
        avalara_dir = quarter_dir / "avalara"
        if avalara_dir.exists():
            downloaded["avalara"] = list(avalara_dir.glob("*.csv"))
        state_gov_dir = quarter_dir / "state_gov"
        if state_gov_dir.exists():
            downloaded["state_gov"] = list(state_gov_dir.glob("*.csv"))

    # Parse each source.
    sources = {}

    sst_files = downloaded.get("sst", [])
    if sst_files:
        logger.info("Parsing %d SST files", len(sst_files))
        sources["sst"] = parse_sst(sst_files)

    avalara_files = downloaded.get("avalara", [])
    if avalara_files:
        logger.info("Parsing %d Avalara files", len(avalara_files))
        sources["avalara"] = parse_avalara(avalara_files)

    state_gov_files = downloaded.get("state_gov", [])
    if state_gov_files:
        logger.info("Parsing %d state_gov files", len(state_gov_files))
        sources["state_gov"] = parse_state_gov(state_gov_files)

    # Merge across sources with priority.
    jurisdictions, rates, zips = merge_sources(sources, SOURCE_PRIORITY)

    _state["jurisdictions"] = jurisdictions
    _state["rates"] = rates
    _state["zip_to_jurisdictions"] = zips

    logger.info(
        "Parsed: %d jurisdictions, %d rates, %d ZIP mappings",
        len(jurisdictions), len(rates), len(zips),
    )


def _run_validate() -> bool:
    """Validate parsed data. Returns True if validation passed."""
    logger.info("=" * 60)
    logger.info("STAGE: VALIDATE")
    logger.info("=" * 60)

    from validate import validate

    jurisdictions = _state.get("jurisdictions")
    rates = _state.get("rates")
    zips = _state.get("zip_to_jurisdictions")

    if jurisdictions is None or rates is None or zips is None:
        logger.error("No parsed data in state — did you run the parse stage first?")
        sys.exit(1)

    result = validate(jurisdictions, rates, zips)
    print("\n" + result.summary() + "\n")

    _state["validation_result"] = result
    return result.passed


def _run_diff() -> None:
    """Generate diff report comparing new data to production."""
    logger.info("=" * 60)
    logger.info("STAGE: DIFF")
    logger.info("=" * 60)

    from diff import generate_diff_report

    jurisdictions = _state.get("jurisdictions")
    rates = _state.get("rates")
    zips = _state.get("zip_to_jurisdictions")

    if jurisdictions is None or rates is None or zips is None:
        logger.error("No parsed data in state — did you run the parse stage first?")
        sys.exit(1)

    report = generate_diff_report(jurisdictions, rates, zips)
    print("\n" + report)

    _state["diff_report"] = report


def _run_load() -> None:
    """Load validated data into staging tables."""
    logger.info("=" * 60)
    logger.info("STAGE: LOAD")
    logger.info("=" * 60)

    from load import load_staging

    jurisdictions = _state.get("jurisdictions")
    rates = _state.get("rates")
    zips = _state.get("zip_to_jurisdictions")

    if jurisdictions is None or rates is None or zips is None:
        logger.error("No parsed data in state — did you run the parse stage first?")
        sys.exit(1)

    load_staging(jurisdictions, rates, zips)
    logger.info("Data loaded into staging tables. Review the diff report before promoting.")


def _run_promote(confirm: bool) -> None:
    """Promote staging data to production."""
    logger.info("=" * 60)
    logger.info("STAGE: PROMOTE")
    logger.info("=" * 60)

    if not confirm:
        logger.warning(
            "Promote requires --confirm flag. "
            "Review the diff report first, then re-run with: "
            "python run.py --stage promote --confirm"
        )
        return

    from load import promote

    summary = promote()
    logger.info("Promotion summary: %s", summary)
    print("\nPromotion complete:")
    for key, value in summary.items():
        print(f"  {key}: {value:,}")


if __name__ == "__main__":
    main()
