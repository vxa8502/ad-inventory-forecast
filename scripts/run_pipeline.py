"""CLI entrypoint for executing the data extraction and transformation pipeline.

Usage:
    python -m scripts.run_pipeline [--dry-run] [--verbose]
"""

import argparse
import logging
import sys
from pathlib import Path

from config import settings
from config.helpers import format_sql_list, get_table_id
from src import bq_client
from src.logging_config import setup_logging
from src.sql_runner import run_sql_file
from src.validators import run_all_validations

setup_logging()
logger = logging.getLogger(__name__)

SEPARATOR = "." * 60
STATUS_ICONS = {"PASS": "[PASS]", "FAIL": "[FAIL]", "WARN": "[WARN]"}


def get_default_params() -> dict[str, str]:
    """Build default parameter dictionary for SQL rendering."""
    return {
        "project_id": settings.PROJECT_ID,
        "dataset": settings.DATASET,
        "location": settings.LOCATION,
        "date_start": settings.DATE_START,
        "date_end": settings.DATE_END,
        "article_list": format_sql_list(settings.ARTICLES),
    }


def _print_header(dry_run: bool) -> None:
    """Print pipeline execution header."""
    print(f"\n{SEPARATOR}")
    print("Ad Inventory Forecast - Data Pipeline")
    print(f"Mode: {'DRY RUN (cost estimation)' if dry_run else 'EXECUTION'}")
    print(f"{SEPARATOR}\n")


def _print_footer() -> None:
    """Print pipeline execution footer."""
    print(f"{SEPARATOR}\n")


def _execute_step(
    step_name: str,
    sql_path: Path,
    params: dict[str, str],
    dry_run: bool,
) -> float:
    """Execute a single pipeline step.

    Args:
        step_name: Human-readable step description.
        sql_path: Path to SQL file.
        params: Parameters for SQL rendering.
        dry_run: If True, estimate cost without executing.

    Returns:
        Estimated cost in USD (0.0 if not dry_run or on failure).
    """
    print(f"{step_name}...")
    logger.debug("Executing step: %s from %s", step_name, sql_path)

    if not sql_path.exists():
        print(f"  -> SQL file not found: {sql_path}")
        logger.warning("SQL file not found: %s", sql_path)
        return 0.0

    try:
        result = run_sql_file(sql_path, dry_run=dry_run, **params)

        if dry_run:
            cost = result.get("estimated_cost_usd", 0)
            gb = result.get("gb_processed", 0)
            print(f"  -> Estimated: {gb:.3f} GB, ${cost:.4f}")
            return cost

        print("  -> Completed successfully")
        return 0.0

    except Exception as e:
        print(f"  -> Failed: {e}")
        logger.exception("Step failed: %s", step_name)
        return 0.0


def _upload_holidays() -> None:
    """Upload US holidays reference CSV to BigQuery."""
    print("Uploading US holidays reference data...")
    logger.info("Uploading holidays CSV")

    csv_path = settings.PROJECT_ROOT / "data" / "reference" / "us_holidays.csv"
    table_id = get_table_id("us_holidays")

    try:
        bq_client.load_csv_to_table(str(csv_path), table_id)
        print("  -> Uploaded successfully")
    except Exception as e:
        print(f"  -> Upload failed: {e}")
        logger.exception("Holiday upload failed")


def _run_validations() -> None:
    """Execute data quality validations and print results."""
    print("\nRunning data quality validations...\n")
    logger.info("Starting validation checks")

    try:
        results = run_all_validations()
        passed = sum(1 for r in results if r["status"] == "PASS")
        total = len(results)

        for r in results:
            status_icon = STATUS_ICONS.get(r["status"], "[????]")
            print(f"  {status_icon} {r['check']}")

        print(f"\nValidation summary: {passed}/{total} checks passed")
        logger.info("Validation complete: %d/%d passed", passed, total)
    except Exception as e:
        print(f"Validation failed: {e}")
        logger.exception("Validation error")


def run_pipeline(dry_run: bool = False) -> None:
    """Execute the full data pipeline.

    Args:
        dry_run: If True, estimate costs without executing queries.
    """
    params = get_default_params()
    sql_dir = settings.PROJECT_ROOT / "sql"
    total_cost = 0.0

    # Note: Google Trends extraction removed. BQ public dataset captures trending
    # breakout events, not persistent topic interest (1/35 article match rate).
    # Calendar features provide cleaner cyclical signals for ARIMA_PLUS_XREG.
    extract_sql = sql_dir / "02_extract" / "wikipedia_pageviews.sql"
    transform_sql = sql_dir / "03_transform" / "build_daily_impressions.sql"
    steps = [
        ("Creating dataset", sql_dir / "01_schema" / "create_dataset.sql"),
        ("Creating tables", sql_dir / "01_schema" / "create_tables.sql"),
        ("Extracting Wikipedia pageviews", extract_sql),
        ("Building daily impressions", transform_sql),
    ]

    _print_header(dry_run)
    logger.info("Starting pipeline in %s mode", "dry-run" if dry_run else "execution")

    for step_name, sql_path in steps:
        cost = _execute_step(step_name, sql_path, params, dry_run)
        total_cost += cost

        if step_name == "Creating tables" and not dry_run:
            _upload_holidays()

    print(f"\n{SEPARATOR}")

    if dry_run:
        print(f"Total estimated cost: ${total_cost:.4f}")
        logger.info("Dry-run complete. Total estimated cost: $%.4f", total_cost)
    else:
        _run_validations()

    _print_footer()


def main() -> None:
    """CLI entrypoint with argument parsing."""
    parser = argparse.ArgumentParser(
        description="Run the ad inventory forecast data pipeline"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Estimate query costs without executing",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose debug logging",
    )

    args = parser.parse_args()

    if args.verbose:
        setup_logging(level=logging.DEBUG, force=True)

    if not settings.PROJECT_ID:
        print("Error: GCP_PROJECT_ID environment variable not set.")
        print("Copy .env.example to .env and configure your project ID.")
        sys.exit(1)

    run_pipeline(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
