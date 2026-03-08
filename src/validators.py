"""Data validation functions for quality assurance."""

import logging
from datetime import date
from typing import Any, TypedDict

from config import settings
from config.helpers import get_table_id
from src import bq_client

__all__ = [
    "ValidationResult",
    "run_all_validations",
    "spot_check_random_rows",
    "validate_date_continuity",
    "validate_holiday_join",
    "validate_no_nulls",
    "validate_row_counts",
]

logger = logging.getLogger(__name__)


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


def _fetch_scalar_result(sql: str) -> Any:
    """Execute query and return first row of results.

    Args:
        sql: SQL query to execute.

    Returns:
        First row of query results.
    """
    result = list(bq_client.run_query(sql))
    return result[0]


def validate_row_counts(table_id: str, expected_min: int) -> ValidationResult:
    """Assert table has at least expected minimum rows.

    Args:
        table_id: Fully qualified table ID.
        expected_min: Minimum expected row count.

    Returns:
        Validation result with status and details.
    """
    sql = f"SELECT COUNT(*) as row_count FROM `{table_id}`"
    row = _fetch_scalar_result(sql)
    actual_count = row.row_count

    status = "PASS" if actual_count >= expected_min else "FAIL"
    logger.info(
        "Row count check: %s (actual=%d, expected_min=%d)",
        status, actual_count, expected_min
    )

    return {
        "check": "row_count",
        "table": table_id,
        "expected_min": expected_min,
        "actual": actual_count,
        "status": status,
    }


def validate_no_nulls(table_id: str, columns: list[str]) -> ValidationResult:
    """Check that specified columns have no NULL values.

    Args:
        table_id: Fully qualified table ID.
        columns: List of column names to check.

    Returns:
        Validation result with status and details.
    """
    null_checks = " OR ".join(f"{col} IS NULL" for col in columns)
    sql = f"SELECT COUNT(*) as null_count FROM `{table_id}` WHERE {null_checks}"

    row = _fetch_scalar_result(sql)
    null_count = row.null_count
    status = "PASS" if null_count == 0 else "FAIL"
    logger.info(
        "Null check: %s (null_count=%d, columns=%s)", status, null_count, columns
    )

    return {
        "check": "no_nulls",
        "table": table_id,
        "columns": columns,
        "null_count": null_count,
        "status": status,
    }


def validate_date_continuity(
    table_id: str, date_col: str, id_col: str
) -> ValidationResult:
    """Detect date gaps within each series.

    Args:
        table_id: Fully qualified table ID.
        date_col: Name of date column.
        id_col: Name of series identifier column.

    Returns:
        Validation result with status and gap count.
    """
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
    gap_count = row.gap_count
    status = "PASS" if gap_count == 0 else "FAIL"
    logger.info("Date continuity check: %s (gap_count=%d)", status, gap_count)

    return {
        "check": "date_continuity",
        "table": table_id,
        "gap_count": gap_count,
        "status": status,
    }


def validate_holiday_join(table_id: str) -> ValidationResult:
    """Verify holiday join produces expected results.

    Checks that:
    1. Some rows have is_holiday=TRUE (join succeeded)
    2. days_to_next_holiday is populated for all rows
    3. is_holiday=TRUE implies days_to_next_holiday=0

    Args:
        table_id: Fully qualified table ID.

    Returns:
        Validation result with holiday join metrics.
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
    holiday_rows = row.holiday_rows
    null_days = row.null_days_count
    edge_violations = row.edge_case_violations

    # PASS if: holidays found, no null days_to_next_holiday, no edge case violations
    is_valid = holiday_rows > 0 and null_days == 0 and edge_violations == 0
    status = "PASS" if is_valid else "FAIL"
    logger.info(
        "Holiday join check: %s (holidays=%d, null_days=%d, edge_violations=%d)",
        status, holiday_rows, null_days, edge_violations
    )

    return {
        "check": "holiday_join",
        "table": table_id,
        "total_rows": row.total_rows,
        "populated": holiday_rows,
        "null_count": null_days,
        "status": status,
    }


def spot_check_random_rows(table_id: str, n: int = 10) -> list[dict]:
    """Fetch random rows for manual inspection.

    Used to visually verify data quality and join correctness.

    Args:
        table_id: Fully qualified table ID.
        n: Number of random rows to fetch.

    Returns:
        List of row dictionaries for inspection.
    """
    sql = f"""
    SELECT *
    FROM `{table_id}`
    ORDER BY RAND()
    LIMIT {n}
    """

    result = bq_client.run_query(sql)
    rows = [dict(row) for row in result]
    logger.info("Spot check: fetched %d random rows from %s", len(rows), table_id)
    return rows


def run_all_validations() -> list[ValidationResult]:
    """Execute all validation checks on daily_impressions table.

    Returns:
        List of validation results with pass/fail status.
    """
    table_id = get_table_id("daily_impressions")

    start = date.fromisoformat(settings.DATE_START)
    end = date.fromisoformat(settings.DATE_END)
    expected_days = (end - start).days + 1
    expected_min_rows = len(settings.ARTICLES) * expected_days

    return [
        validate_row_counts(table_id, expected_min_rows),
        validate_no_nulls(
            table_id, ["date", "ad_unit", "daily_impressions", "day_of_week"]
        ),
        validate_date_continuity(table_id, "date", "ad_unit"),
        validate_holiday_join(table_id),
    ]
