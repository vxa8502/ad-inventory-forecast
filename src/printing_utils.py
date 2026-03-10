"""Consolidated CLI output formatting utilities.

Single source of truth for all console output formatting in the project.
Provides consistent headers, sections, and status formatting.
"""

from __future__ import annotations

import logging
from typing import Any, Callable

__all__ = [
    "SEPARATOR_WIDTH",
    "STATUS_ICONS",
    "format_markdown_table",
    "format_status",
    "print_dataframe_rows",
    "print_footer",
    "print_header",
    "print_pipeline_header",
    "print_section",
    "print_subsection",
    "print_summary_header",
    "print_validation_results",
]

logger = logging.getLogger(__name__)

SEPARATOR_WIDTH = 70
STATUS_ICONS = {
    "PASS": "[PASS]",
    "FAIL": "[FAIL]",
    "WARN": "[WARN]",
    "REVIEW": "[REVIEW]",
    "ERROR": "[ERROR]",
    "MISSING": "[MISSING]",
    "UNKNOWN": "[????]",
}


def _separator(char: str = "=") -> str:
    """Build a separator string of standard width."""
    return char * SEPARATOR_WIDTH


def format_status(status: str) -> str:
    """Format a status string with its corresponding icon.

    Args:
        status: Status string (PASS, FAIL, WARN, REVIEW, ERROR, MISSING, UNKNOWN).

    Returns:
        Formatted string with icon prefix, e.g., "[PASS] PASS".
    """
    icon = STATUS_ICONS.get(status, STATUS_ICONS["UNKNOWN"])
    return f"{icon} {status}"


def print_section(title: str) -> None:
    """Print a major section header.

    Args:
        title: Section title to display.
    """
    sep = _separator("=")
    print(f"\n{sep}")
    print(f"  {title}")
    print(f"{sep}\n")


def print_subsection(title: str, char: str = "-") -> None:
    """Print a minor subsection header.

    Args:
        title: Subsection title to display.
        char: Character to use for the separator line.
    """
    print(f"\n{title}")
    print(_separator(char))


def print_summary_header(title: str) -> None:
    """Print a prominent summary header with hash border.

    Args:
        title: Summary title to display.
    """
    border = _separator("#")
    print(f"\n{border}")
    print(f"  {title}")
    print(border)


def print_header(title: str) -> None:
    """Print a simple section header with dot separator.

    Args:
        title: Header title to display.
    """
    print(f"\n{_separator('.')}")
    print(title)
    print(f"{_separator('.')}\n")


def print_pipeline_header(title: str, dry_run: bool, **extras: str) -> None:
    """Print pipeline execution header with mode indicator.

    Args:
        title: Pipeline title to display.
        dry_run: Whether running in dry-run mode.
        **extras: Additional key-value pairs to display.
    """
    print(f"\n{_separator('.')}")
    print(title)
    print(f"Mode: {'DRY RUN (cost estimation)' if dry_run else 'EXECUTION'}")
    for key, value in extras.items():
        if value:
            print(f"{key}: {value}")
    print(f"{_separator('.')}\n")


def print_footer() -> None:
    """Print pipeline execution footer."""
    print(f"{_separator('.')}\n")


def print_validation_results(results: list[dict], title: str = "Validations") -> None:
    """Print formatted validation results with status icons.

    Args:
        results: List of validation result dicts with 'status' and 'check' keys.
        title: Section title.
    """
    print(f"\nRunning {title.lower()}...\n")

    passed = sum(1 for r in results if r["status"] == "PASS")
    total = len(results)

    for r in results:
        status_icon = STATUS_ICONS.get(r["status"], "[????]")
        fold_info = f" ({r.get('fold', 'all')})" if "fold" in r else ""
        print(f"  {status_icon} {r['check']}{fold_info}")

    print(f"\nValidation summary: {passed}/{total} checks passed")
    logger.info("Validation complete: %d/%d passed", passed, total)


def print_dataframe_rows(
    df: "pd.DataFrame",
    columns: list[str],
    headers: list[str] | None = None,
    widths: list[int] | None = None,
    formatters: dict[str, str] | None = None,
    max_rows: int | None = None,
    truncate_col: str | None = None,
    truncate_len: int = 29,
) -> None:
    """Print DataFrame rows as a formatted CLI table.

    Args:
        df: DataFrame to print.
        columns: Column names to include.
        headers: Display headers (defaults to column names).
        widths: Column widths for alignment.
        formatters: Dict mapping column names to format strings (e.g., "{:>8.1f}%").
        max_rows: Maximum rows to print (None for all).
        truncate_col: Column to truncate values (usually text column).
        truncate_len: Max length for truncated column.
    """
    headers = headers or columns
    widths = widths or [max(len(h), 10) for h in headers]
    formatters = formatters or {}

    # Print header
    header_parts = [f"{h:<{w}}" for h, w in zip(headers, widths)]
    print(" ".join(header_parts))

    # Print rows
    rows_to_print = df.head(max_rows) if max_rows else df
    for _, row in rows_to_print.iterrows():
        parts = []
        for col, width in zip(columns, widths):
            value = row[col]
            if col == truncate_col and isinstance(value, str):
                value = value[:truncate_len]
            if col in formatters:
                formatted = formatters[col].format(value)
            else:
                formatted = str(value) if value is not None else ""
            parts.append(f"{formatted:<{width}}")
        print(" ".join(parts))


def format_markdown_table(
    rows: list[dict[str, Any]],
    columns: list[str],
    headers: list[str] | None = None,
    formatters: dict[str, Callable[[Any], str]] | None = None,
) -> str:
    """Build a markdown table from a list of row dictionaries.

    Args:
        rows: List of dictionaries, each representing a row.
        columns: List of column keys to include from each row dict.
        headers: Optional display headers (defaults to column keys).
        formatters: Optional dict mapping column keys to formatting functions.

    Returns:
        Formatted markdown table as a string.

    Example:
        >>> rows = [{"name": "Alice", "score": 95}, {"name": "Bob", "score": 87}]
        >>> print(format_markdown_table(
        ...     rows,
        ...     columns=["name", "score"],
        ...     headers=["Name", "Score"],
        ...     formatters={"score": lambda x: f"{x:.0f}"}
        ... ))
        | Name | Score |
        |------|-------|
        | Alice | 95 |
        | Bob | 87 |
    """
    if not rows:
        return ""

    formatters = formatters or {}
    headers = headers or columns

    # Build header row
    header_row = "| " + " | ".join(headers) + " |"
    separator = "|" + "|".join("-" * (len(h) + 2) for h in headers) + "|"

    # Build data rows
    data_rows = []
    for row in rows:
        cells = []
        for col in columns:
            value = row.get(col, "")
            if col in formatters:
                value = formatters[col](value)
            else:
                value = str(value) if value is not None else ""
            cells.append(value)
        data_rows.append("| " + " | ".join(cells) + " |")

    return "\n".join([header_row, separator] + data_rows)
