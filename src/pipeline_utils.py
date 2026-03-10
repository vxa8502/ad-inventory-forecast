"""Shared utilities for pipeline execution scripts."""

from __future__ import annotations

import logging
from pathlib import Path

from config import settings
from src.printing_utils import (
    SEPARATOR_WIDTH,
    STATUS_ICONS,
    print_footer,
    print_pipeline_header,
    print_validation_results,
)
from src.sql_runner import run_sql_file

__all__ = [
    "PIPELINE_EXCEPTIONS",
    "SEPARATOR_WIDTH",
    "STATUS_ICONS",
    "execute_sql_step",
    "get_base_params",
    "print_footer",
    "print_header",
    "print_validation_results",
]

logger = logging.getLogger(__name__)

# Canonical exception tuple for pipeline error handling
# Use this consistently across all pipeline scripts for uniform error handling
PIPELINE_EXCEPTIONS = (RuntimeError, KeyError, TypeError, ValueError, FileNotFoundError)

# Re-export for backwards compatibility
print_header = print_pipeline_header


def get_base_params() -> dict[str, str]:
    """Build base parameter dictionary with project and dataset."""
    return {
        "project_id": settings.PROJECT_ID,
        "dataset": settings.DATASET,
    }


def execute_sql_step(
    step_name: str,
    sql_path: Path,
    params: dict[str, str],
    dry_run: bool,
    indent: int = 0,
) -> float:
    """Execute a single pipeline step.

    Args:
        step_name: Human-readable step description.
        sql_path: Path to SQL file.
        params: Parameters for SQL rendering.
        dry_run: If True, estimate cost without executing.
        indent: Number of spaces to indent output.

    Returns:
        Estimated cost in USD (0.0 if not dry_run or on failure).
    """
    prefix = " " * indent
    print(f"{prefix}{step_name}...")
    logger.debug("Executing step: %s from %s", step_name, sql_path)

    if not sql_path.exists():
        print(f"{prefix}  -> SQL file not found: {sql_path}")
        logger.warning("SQL file not found: %s", sql_path)
        return 0.0

    try:
        result = run_sql_file(sql_path, dry_run=dry_run, **params)

        if dry_run:
            cost = result.get("estimated_cost_usd", 0)
            gb = result.get("gb_processed", 0)
            print(f"{prefix}  -> Estimated: {gb:.3f} GB, ${cost:.4f}")
            return cost

        print(f"{prefix}  -> Completed successfully")
        return 0.0

    except PIPELINE_EXCEPTIONS as e:
        print(f"{prefix}  -> Failed: {e}")
        logger.exception("Step failed: %s", step_name)
        if not dry_run:
            raise
        return 0.0
