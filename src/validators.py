"""Data validation functions for quality assurance."""

from __future__ import annotations

import logging
from typing import Any, TypedDict

from config import settings
from config.helpers import get_date_range, get_table_id
from config.settings import FoldName
from src import bq_client
from src.bq_client import query_to_list

__all__ = [
    "ValidationResult",
    "ensure_dataframe_has_data",
    "run_all_validations",
    "spot_check_random_rows",
    "validate_date_continuity",
    "validate_forecast_coverage",
    "validate_holiday_join",
    "validate_metrics_completeness",
    "validate_no_infinite_metrics",
    "validate_no_nulls",
    "validate_row_counts",
]

logger = logging.getLogger(__name__)

# Valid fold names for SQL parameter validation
VALID_FOLD_NAMES = frozenset(f["name"] for f in settings.FOLD_CONFIGS)


def ensure_dataframe_has_data(
    df: "pd.DataFrame",
    context: str,
    log_warning: bool = True,
) -> bool:
    """Check if DataFrame has data and optionally log a warning if empty.

    Args:
        df: DataFrame to check.
        context: Description of what this DataFrame represents (for logging).
        log_warning: Whether to log a warning if empty.

    Returns:
        True if DataFrame has data, False if empty.
    """
    # Import here to avoid circular imports
    import pandas as pd

    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        if log_warning:
            logger.warning("No data returned for %s", context)
        return False
    return True


class ValidationResult(TypedDict, total=False):
    """Typed dictionary for validation check results."""

    check: str
    table: str
    status: str
    expected_min: int
    actual: int
    columns: list[str]
    null_count: int
    gap_count: int
    total_rows: int
    populated: int
    coverage_pct: float
    min_coverage_pct: float
    fold: str


def _fetch_scalar_result(sql: str) -> Any:
    """Execute query and return first row of results."""
    result = bq_client.run_query(sql)
    return next(iter(result))


def _validate_fold_name(fold_name: FoldName) -> None:
    """Validate fold_name is a known fold from settings.

    Raises:
        ValueError: If fold_name is not in FOLD_CONFIGS.
    """
    if fold_name not in VALID_FOLD_NAMES:
        raise ValueError(
            f"Invalid fold_name '{fold_name}'. "
            f"Valid options: {sorted(VALID_FOLD_NAMES)}"
        )


def validate_row_counts(table_id: str, expected_min: int) -> ValidationResult:
    """Assert table has at least expected minimum rows."""
    sql = f"SELECT COUNT(*) as row_count FROM `{table_id}`"
    row = _fetch_scalar_result(sql)
    passed = row.row_count >= expected_min

    logger.info("Row count check: %s (actual=%d)", "PASS" if passed else "FAIL", row.row_count)

    return {
        "check": "row_count",
        "table": table_id,
        "status": "PASS" if passed else "FAIL",
        "expected_min": expected_min,
        "actual": row.row_count,
    }


def validate_no_nulls(table_id: str, columns: list[str]) -> ValidationResult:
    """Check that specified columns have no NULL values."""
    null_checks = " OR ".join(f"{col} IS NULL" for col in columns)
    sql = f"SELECT COUNT(*) as null_count FROM `{table_id}` WHERE {null_checks}"
    row = _fetch_scalar_result(sql)
    passed = row.null_count == 0

    logger.info("Null check: %s (null_count=%d)", "PASS" if passed else "FAIL", row.null_count)

    return {
        "check": "no_nulls",
        "table": table_id,
        "status": "PASS" if passed else "FAIL",
        "columns": columns,
        "null_count": row.null_count,
    }


def validate_date_continuity(
    table_id: str, date_col: str, id_col: str
) -> ValidationResult:
    """Detect date gaps within each series."""
    lag_window = f"PARTITION BY {id_col} ORDER BY {date_col}"
    sql = f"""
    WITH date_gaps AS (
        SELECT
            {id_col},
            {date_col},
            LAG({date_col}) OVER ({lag_window}) AS prev_date,
            DATE_DIFF(
                {date_col},
                LAG({date_col}) OVER ({lag_window}),
                DAY
            ) AS day_diff
        FROM `{table_id}`
    )
    SELECT COUNT(*) as gap_count
    FROM date_gaps
    WHERE day_diff > 1
    """
    row = _fetch_scalar_result(sql)
    passed = row.gap_count == 0

    logger.info("Date continuity check: %s (gap_count=%d)", "PASS" if passed else "FAIL", row.gap_count)

    return {
        "check": "date_continuity",
        "table": table_id,
        "status": "PASS" if passed else "FAIL",
        "gap_count": row.gap_count,
    }


def validate_holiday_join(table_id: str) -> ValidationResult:
    """Verify holiday join produces expected results.

    Checks that:
    1. Some rows have is_holiday=TRUE (join succeeded)
    2. days_to_next_holiday is populated for all rows
    3. is_holiday=TRUE implies days_to_next_holiday=0
    """
    sql = f"""
    SELECT
        COUNT(*) as total_rows,
        COUNTIF(is_holiday) as holiday_rows,
        COUNTIF(days_to_next_holiday IS NULL) as null_days_count,
        COUNTIF(is_holiday AND days_to_next_holiday != 0) as edge_case_violations
    FROM `{table_id}`
    """
    row = _fetch_scalar_result(sql)
    passed = row.holiday_rows > 0 and row.null_days_count == 0 and row.edge_case_violations == 0

    logger.info(
        "Holiday join check: %s (holidays=%d, null_days=%d)",
        "PASS" if passed else "FAIL",
        row.holiday_rows,
        row.null_days_count,
    )

    return {
        "check": "holiday_join",
        "table": table_id,
        "status": "PASS" if passed else "FAIL",
        "total_rows": row.total_rows,
        "populated": row.holiday_rows,
        "null_count": row.null_days_count,
    }


def spot_check_random_rows(table_id: str, n: int = 10) -> list[dict[str, Any]]:
    """Fetch random rows for manual inspection."""
    sql = f"""
    SELECT *
    FROM `{table_id}`
    ORDER BY RAND()
    LIMIT {n}
    """
    rows = query_to_list(sql)
    logger.info("Spot check: fetched %d random rows from %s", len(rows), table_id)
    return rows


def run_all_validations() -> list[ValidationResult]:
    """Execute all validation checks on daily_impressions table."""
    table_id = get_table_id("daily_impressions")

    _, _, expected_days = get_date_range()
    expected_min_rows = len(settings.ARTICLES) * expected_days

    return [
        validate_row_counts(table_id, expected_min_rows),
        validate_no_nulls(
            table_id, ["date", "ad_unit", "daily_impressions", "day_of_week"]
        ),
        validate_date_continuity(table_id, "date", "ad_unit"),
        validate_holiday_join(table_id),
    ]


def _get_expected_model_count() -> int:
    """Return expected model count based on TIMESFM_ENABLED setting."""
    if settings.TIMESFM_ENABLED:
        return len(settings.MODEL_NAMES)
    return len([m for m in settings.MODEL_NAMES if m != "timesfm_2_5"])


def validate_forecast_coverage(fold_name: FoldName) -> ValidationResult:
    """Verify all models produced forecasts for all ad units.

    Raises:
        ValueError: If fold_name is not a valid fold.
    """
    _validate_fold_name(fold_name)

    table_id = get_table_id("forecasts")
    expected_models = _get_expected_model_count()
    expected_ad_units = len(settings.ARTICLES)

    sql = f"""
    SELECT
        COUNT(DISTINCT model_name) AS model_count,
        COUNT(DISTINCT ad_unit) AS ad_unit_count,
        COUNT(*) AS total_forecasts
    FROM `{table_id}`
    WHERE fold_name = '{fold_name}'
    """
    row = _fetch_scalar_result(sql)
    passed = row.model_count == expected_models and row.ad_unit_count == expected_ad_units

    logger.info(
        "Forecast coverage check (%s): %s (total=%d)",
        fold_name,
        "PASS" if passed else "FAIL",
        row.total_forecasts,
    )

    return {
        "check": "forecast_coverage",
        "table": table_id,
        "status": "PASS" if passed else "FAIL",
        "actual": row.total_forecasts,
        "fold": fold_name,
    }


def validate_metrics_completeness(fold_name: FoldName) -> ValidationResult:
    """Verify all metrics computed for all model x ad_unit combinations.

    Raises:
        ValueError: If fold_name is not a valid fold.
    """
    _validate_fold_name(fold_name)

    table_id = get_table_id("model_metrics")
    expected_models = _get_expected_model_count()
    expected_ad_units = len(settings.ARTICLES)
    expected_metrics = len(settings.METRIC_NAMES)

    sql = f"""
    SELECT
        COUNT(DISTINCT model_name) AS model_count,
        COUNT(DISTINCT ad_unit) AS ad_unit_count,
        COUNT(DISTINCT metric_name) AS metric_count,
        COUNT(*) AS total_rows
    FROM `{table_id}`
    WHERE fold_name = '{fold_name}'
    """
    row = _fetch_scalar_result(sql)
    passed = (
        row.model_count == expected_models
        and row.ad_unit_count == expected_ad_units
        and row.metric_count == expected_metrics
    )

    logger.info(
        "Metrics completeness check (%s): %s (total=%d)",
        fold_name,
        "PASS" if passed else "FAIL",
        row.total_rows,
    )

    return {
        "check": "metrics_completeness",
        "table": table_id,
        "status": "PASS" if passed else "FAIL",
        "actual": row.total_rows,
        "fold": fold_name,
    }


def validate_no_infinite_metrics(fold_name: FoldName) -> ValidationResult:
    """Check for NaN or Inf values in metrics.

    MASE is allowed to be NULL for constant series (naive baseline = 0).
    All other metrics must be finite and non-NULL.

    Raises:
        ValueError: If fold_name is not a valid fold.
    """
    _validate_fold_name(fold_name)

    table_id = get_table_id("model_metrics")

    sql = f"""
    SELECT COUNT(*) AS invalid_count
    FROM `{table_id}`
    WHERE fold_name = '{fold_name}'
      AND (
          IS_NAN(metric_value)
          OR IS_INF(metric_value)
          OR (metric_value IS NULL AND metric_name != 'mase')
      )
    """
    row = _fetch_scalar_result(sql)
    passed = row.invalid_count == 0

    logger.info(
        "Infinite metrics check (%s): %s (invalid_count=%d)",
        fold_name,
        "PASS" if passed else "FAIL",
        row.invalid_count,
    )

    return {
        "check": "no_infinite_metrics",
        "table": table_id,
        "status": "PASS" if passed else "FAIL",
        "null_count": row.invalid_count,
        "fold": fold_name,
    }
