"""Shared helper utilities for the ad inventory forecasting project."""

from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd

from config import settings

__all__ = [
    "date_range_days",
    "format_sql_list",
    "get_date_range",
    "get_model_id",
    "get_table_id",
    "safe_dataframe_extract",
    "table_ref",
]


def get_date_range() -> tuple[date, date, int]:
    """Get configured date range and expected day count.

    Returns:
        Tuple of (start_date, end_date, expected_days).

    Example:
        >>> start, end, days = get_date_range()
        >>> print(f"Training window: {start} to {end} ({days} days)")
    """
    start = date.fromisoformat(settings.DATE_START)
    end = date.fromisoformat(settings.DATE_END)
    expected_days = (end - start).days + 1
    return start, end, expected_days


def get_table_id(table_name: str) -> str:
    """Build fully qualified BigQuery table ID.

    Args:
        table_name: Name of the table (e.g., 'daily_impressions').

    Returns:
        Fully qualified table ID: project.dataset.table
    """
    return f"{settings.PROJECT_ID}.{settings.DATASET}.{table_name}"


def table_ref(table_name: str) -> str:
    """Build backtick-quoted table reference for SQL queries.

    Args:
        table_name: Name of the table (e.g., 'daily_impressions').

    Returns:
        Backtick-quoted reference: `project.dataset.table`
    """
    return f"`{settings.PROJECT_ID}.{settings.DATASET}.{table_name}`"


def date_range_days(start: date, end: date) -> int:
    """Calculate inclusive day count between two dates.

    Args:
        start: Start date (inclusive).
        end: End date (inclusive).

    Returns:
        Number of days in the range.
    """
    return (end - start).days + 1


def format_sql_list(items: list[str]) -> str:
    """Format a list of strings for SQL IN clause.

    Args:
        items: List of string values.

    Returns:
        Comma-separated, single-quoted string for SQL IN clause.

    Example:
        >>> format_sql_list(["a", "b", "c"])
        "'a', 'b', 'c'"
    """
    return ", ".join(f"'{item}'" for item in items)


def get_model_id(model_name: str, fold_name: str) -> str:
    """Build fully qualified BigQuery model ID.

    Args:
        model_name: Name of the model (e.g., 'arima_plus').
        fold_name: Name of the fold (e.g., 'fold_1').

    Returns:
        Fully qualified model ID: project.dataset.model_fold
    """
    return f"{settings.PROJECT_ID}.{settings.DATASET}.{model_name}_{fold_name}"


def safe_dataframe_extract(
    df: pd.DataFrame,
    filter_col: str,
    filter_val: Any,
    value_col: str,
) -> float | None:
    """Safely extract a single value from a filtered DataFrame.

    Filters the DataFrame by a column value and extracts a scalar from
    the result. Returns None if no matching rows exist, avoiding IndexError.

    Args:
        df: DataFrame to filter.
        filter_col: Column name to filter on.
        filter_val: Value to match in filter column.
        value_col: Column to extract value from.

    Returns:
        The extracted value as float, or None if no matching rows.

    Example:
        >>> effect = safe_dataframe_extract(df, "year", 2023, "weekend_effect_pct")
        >>> if effect is not None:
        ...     print(f"2023 effect: {effect:.1f}%")
    """
    filtered = df[df[filter_col] == filter_val][value_col].values
    return float(filtered[0]) if len(filtered) > 0 else None
