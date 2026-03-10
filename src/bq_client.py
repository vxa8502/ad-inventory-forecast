"""BigQuery client wrapper with cost controls and utility functions."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from google.cloud import bigquery

if TYPE_CHECKING:
    from pathlib import Path

    import pandas as pd

from config import settings

__all__ = [
    "estimate_query_cost",
    "get_client",
    "load_csv_to_table",
    "query_to_dataframe",
    "query_to_list",
    "run_query",
]

logger = logging.getLogger(__name__)

BYTES_PER_GB = 1024**3
BYTES_PER_TB = 1024**4
BIGQUERY_COST_PER_TB_USD = 5.0
DEFAULT_QUERY_TIMEOUT_SECONDS = 300  # 5 minutes


def get_client() -> bigquery.Client:
    """Initialize BigQuery client with project configuration."""
    logger.debug("Initializing BigQuery client for project=%s", settings.PROJECT_ID)
    return bigquery.Client(project=settings.PROJECT_ID, location=settings.LOCATION)


def _is_dml_statement(sql: str) -> bool:
    """Check if SQL contains DML that cannot be dry-run estimated.

    BigQuery dry-run only supports SELECT queries. MERGE, INSERT (standalone),
    UPDATE, and DELETE statements cannot be cost-estimated via dry-run.
    """
    return sql.strip().upper().startswith(("MERGE", "INSERT", "UPDATE"))


def estimate_query_cost(sql: str) -> dict[str, float]:
    """Perform dry-run to estimate query cost before execution.

    Args:
        sql: SQL query string to estimate.

    Returns:
        Dictionary with bytes_processed, gb_processed, and estimated_cost_usd.

    Note:
        DML statements (MERGE, standalone INSERT/UPDATE) cannot be dry-run
        estimated. Returns zero-cost placeholder for these.
    """
    if _is_dml_statement(sql):
        logger.info("DML statement detected - skipping dry-run (cost unknown)")
        return {
            "bytes_processed": 0,
            "gb_processed": 0.0,
            "estimated_cost_usd": 0.0,
            "note": "DML statement - cost estimated at execution time",
        }

    client = get_client()
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)

    logger.debug("Running dry-run cost estimation")
    query_job = client.query(sql, job_config=job_config)
    bytes_processed = query_job.total_bytes_processed

    estimated_cost = (bytes_processed / BYTES_PER_TB) * BIGQUERY_COST_PER_TB_USD
    gb_processed = round(bytes_processed / BYTES_PER_GB, 3)

    logger.info(
        "Cost estimate: %.3f GB, $%.4f USD", gb_processed, estimated_cost
    )

    return {
        "bytes_processed": bytes_processed,
        "gb_processed": gb_processed,
        "estimated_cost_usd": round(estimated_cost, 4),
    }


def run_query(
    sql: str, dry_run: bool = False
) -> bigquery.table.RowIterator | dict[str, float]:
    """Execute query with MAX_BYTES_BILLED cost guard.

    Args:
        sql: SQL query string to execute.
        dry_run: If True, return cost estimate instead of executing.

    Returns:
        Query results as RowIterator, or cost estimate dict if dry_run.
    """
    if dry_run:
        return estimate_query_cost(sql)

    client = get_client()
    job_config = bigquery.QueryJobConfig(
        maximum_bytes_billed=settings.MAX_BYTES_BILLED
    )

    logger.info(
        "Executing query with cost guard: %d GB max",
        settings.MAX_BYTES_BILLED // BYTES_PER_GB,
    )
    query_job = client.query(sql, job_config=job_config)
    result = query_job.result(timeout=DEFAULT_QUERY_TIMEOUT_SECONDS)
    logger.info(
        "Query completed: %d rows returned, %.3f GB processed",
        result.total_rows or 0,
        (query_job.total_bytes_processed or 0) / BYTES_PER_GB,
    )
    return result


def query_to_dataframe(sql: str, description: str | None = None) -> pd.DataFrame:
    """Execute query and return results as pandas DataFrame.

    Convenience wrapper combining run_query() with to_dataframe() conversion.

    Args:
        sql: SQL query string to execute.
        description: Optional description for logging.

    Returns:
        Query results as pandas DataFrame.
    """
    # Lazy import: pandas is only needed when this function is called,
    # not for basic BigQuery operations like run_query() or load_csv_to_table()
    import pandas as pd  # noqa: PLC0415

    if description:
        logger.info("%s...", description)
    result = run_query(sql)
    if isinstance(result, dict):
        return pd.DataFrame()
    return result.to_dataframe()


def query_to_list(sql: str, description: str | None = None) -> list[dict]:
    """Execute query and return results as list of row dictionaries.

    Convenience wrapper for queries where DataFrame overhead is unnecessary.

    Args:
        sql: SQL query string to execute.
        description: Optional description for logging.

    Returns:
        Query results as list of dictionaries (one per row).
    """
    if description:
        logger.info("%s...", description)
    result = run_query(sql)
    if isinstance(result, dict):
        return []
    return [dict(row) for row in result]


def load_csv_to_table(csv_path: str | Path, table_id: str) -> bigquery.LoadJob:
    """Upload CSV file to BigQuery table.

    Args:
        csv_path: Path to local CSV file.
        table_id: Fully qualified table ID (project.dataset.table).

    Returns:
        Completed LoadJob object.
    """
    client = get_client()
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    logger.info("Loading CSV to %s from %s", table_id, csv_path)
    with open(csv_path, "rb") as source_file:  # noqa: PTH123
        load_job = client.load_table_from_file(
            source_file, table_id, job_config=job_config
        )

    load_job.result()
    logger.info(
        "CSV load completed: %d rows loaded to %s",
        load_job.output_rows or 0,
        table_id,
    )
    return load_job
