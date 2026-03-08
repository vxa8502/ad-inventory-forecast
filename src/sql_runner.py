"""SQL file loading and execution utilities."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from config import settings
from src import bq_client

if TYPE_CHECKING:
    from google.cloud import bigquery

__all__ = ["load_sql", "render_sql", "run_sql_file"]


def load_sql(filepath: str | Path) -> str:
    """Read SQL file contents as string.

    Args:
        filepath: Path to SQL file, relative to project root or absolute.

    Returns:
        SQL file contents as string.
    """
    path = Path(filepath)
    if not path.is_absolute():
        path = settings.PROJECT_ROOT / path

    return path.read_text()


def render_sql(sql: str, **params) -> str:
    """Substitute {placeholders} with parameter values.

    Args:
        sql: SQL string with {placeholder} markers.
        **params: Key-value pairs for substitution.

    Returns:
        SQL string with placeholders replaced.
    """
    return sql.format(**params)


def run_sql_file(
    filepath: str | Path, dry_run: bool = False, **params: str
) -> bigquery.table.RowIterator | dict[str, float]:
    """Load SQL file, render with parameters, and execute.

    Args:
        filepath: Path to SQL file.
        dry_run: If True, return cost estimate instead of executing.
        **params: Parameters for placeholder substitution.

    Returns:
        Query results as RowIterator, or cost estimate dict if dry_run.
    """
    sql = load_sql(filepath)
    rendered = render_sql(sql, **params)
    return bq_client.run_query(rendered, dry_run=dry_run)
