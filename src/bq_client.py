"""BigQuery client wrapper with cost controls and utility functions."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from google.cloud import bigquery

if TYPE_CHECKING:
    from pathlib import Path

from config import settings

__all__ = [
    "estimate_query_cost",
    "get_client",
    "load_csv_to_table",
    "run_query",
]

logger = logging.getLogger(__name__)

BYTES_PER_GB = 1024**3
BYTES_PER_TB = 1024**4
BIGQUERY_COST_PER_TB_USD = 5.0


def get_client() -> bigquery.Client:
    """Initialize BigQuery client with project configuration."""
    logger.debug("Initializing BigQuery client for project=%s", settings.PROJECT_ID)
    return bigquery.Client(project=settings.PROJECT_ID, location=settings.LOCATION)


def estimate_query_cost(sql: str) -> dict[str, float]:
    """Perform dry-run to estimate query cost before execution.

    Args:
        sql: SQL query string to estimate.

    Returns:
        Dictionary with bytes_processed, gb_processed, and estimated_cost_usd.
    """
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
    result = query_job.result()
    logger.info(
        "Query completed: %d rows returned, %.3f GB processed",
        result.total_rows or 0,
        (query_job.total_bytes_processed or 0) / BYTES_PER_GB,
    )
    return result


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
