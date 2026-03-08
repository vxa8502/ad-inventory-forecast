"""Shared helper utilities for the ad inventory forecasting project."""

from config import settings

__all__ = [
    "format_sql_list",
    "get_table_id",
]


def get_table_id(table_name: str) -> str:
    """Build fully qualified BigQuery table ID.

    Args:
        table_name: Name of the table (e.g., 'daily_impressions').

    Returns:
        Fully qualified table ID: project.dataset.table
    """
    return f"{settings.PROJECT_ID}.{settings.DATASET}.{table_name}"


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
