"""CLI entrypoint for executing the data extraction and transformation pipeline.

Usage:
    python -m scripts.run_pipeline [--dry-run] [--verbose]
"""

from __future__ import annotations

import argparse
import logging

from config import settings
from config.helpers import format_sql_list, get_table_id
from src import bq_client
from src.cli import add_common_args, configure_logging_from_args, require_project_id
from src.logging_config import setup_logging
from src.pipeline_utils import (
    PIPELINE_EXCEPTIONS,
    SEPARATOR_WIDTH,
    execute_sql_step,
    get_base_params,
    print_footer,
    print_header,
    print_validation_results,
)
from src.validators import run_all_validations

setup_logging()
logger = logging.getLogger(__name__)


def get_default_params() -> dict[str, str]:
    """Build default parameter dictionary for SQL rendering."""
    params = get_base_params()
    params.update({
        "location": settings.LOCATION,
        "date_start": settings.DATE_START,
        "date_end": settings.DATE_END,
        "article_list": format_sql_list(settings.ARTICLES),
    })
    return params


def _upload_holidays() -> bool:
    """Upload US holidays reference CSV to BigQuery.

    Returns:
        True if upload succeeded, False otherwise.
    """
    print("Uploading US holidays reference data...")

    csv_path = settings.PROJECT_ROOT / "data" / "reference" / "us_holidays.csv"
    table_id = get_table_id("us_holidays")

    try:
        bq_client.load_csv_to_table(str(csv_path), table_id)
        print("  -> Uploaded successfully")
        logger.info("Holidays CSV uploaded to %s", table_id)
        return True
    except FileNotFoundError:
        print(f"  -> CSV not found: {csv_path}")
        logger.error("Holiday CSV not found: %s", csv_path)
        return False
    except PermissionError as e:
        print(f"  -> Permission denied: {e}")
        logger.error("Permission denied for CSV: %s", csv_path)
        return False
    except (OSError, RuntimeError) as e:
        print(f"  -> Upload failed: {e}")
        logger.exception("Holiday upload failed")
        return False


def _run_validations() -> None:
    """Execute data quality validations and print results."""
    try:
        results = run_all_validations()
        print_validation_results(results, "data quality validations")
    except PIPELINE_EXCEPTIONS as e:
        print(f"Validation failed: {e}")
        logger.exception("Validation error: %s", e)


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
    steps = [
        ("Creating dataset", sql_dir / "01_schema" / "create_dataset.sql"),
        ("Creating tables", sql_dir / "01_schema" / "create_tables.sql"),
        ("Extracting Wikipedia pageviews", sql_dir / "02_extract" / "wikipedia_pageviews.sql"),
        ("Building daily impressions", sql_dir / "03_transform" / "build_daily_impressions.sql"),
        ("Backfilling date gaps", sql_dir / "03_transform" / "backfill_gaps.sql"),
    ]

    print_header("Ad Inventory Forecast - Data Pipeline", dry_run)
    logger.info("Starting pipeline in %s mode", "dry-run" if dry_run else "execution")

    for step_name, sql_path in steps:
        cost = execute_sql_step(step_name, sql_path, params, dry_run)
        total_cost += cost

        if step_name == "Creating tables" and not dry_run:
            _upload_holidays()

    print(f"\n{'.' * SEPARATOR_WIDTH}")

    if dry_run:
        print(f"Total estimated cost: ${total_cost:.4f}")
        logger.info("Dry-run complete. Total estimated cost: $%.4f", total_cost)
    else:
        _run_validations()

    print_footer()


def main() -> None:
    """CLI entrypoint with argument parsing."""
    parser = argparse.ArgumentParser(
        description="Run the ad inventory forecast data pipeline"
    )
    add_common_args(parser)

    args = parser.parse_args()
    configure_logging_from_args(args)
    require_project_id()

    run_pipeline(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
