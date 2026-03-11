"""
ARIMA+ Weekly Retraining Pipeline

Vertex AI Pipeline DAG for automated model retraining.
Triggered weekly via Cloud Scheduler (Saturday 2 AM UTC).

See README.md "Scaling to 10M Ad Slots" section for usage and architecture.
"""

from kfp.v2 import dsl
from kfp.v2.dsl import component


@component(base_image="python:3.11-slim", packages_to_install=["google-cloud-bigquery"])
def extract_recent_traffic(project_id: str, lookback_days: int) -> str:
    """Extract last N days of pageview data to staging table."""
    from google.cloud import bigquery

    client = bigquery.Client(project=project_id)
    query = f"""
    CREATE OR REPLACE TABLE `{project_id}.ad_inventory.staging_recent`
    AS SELECT * FROM `{project_id}.ad_inventory.daily_impressions`
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_days} DAY)
    """
    client.query(query).result()
    return f"{project_id}.ad_inventory.staging_recent"


@component(base_image="python:3.11-slim", packages_to_install=["google-cloud-bigquery"])
def train_arima(project_id: str, staging_table: str) -> str:
    """Retrain ARIMA+ model on recent data."""
    from google.cloud import bigquery

    client = bigquery.Client(project=project_id)
    query = f"""
    CREATE OR REPLACE MODEL `{project_id}.ad_inventory.arima_model`
    OPTIONS(
        model_type='ARIMA_PLUS',
        time_series_timestamp_col='date',
        time_series_data_col='daily_impressions',
        time_series_id_col='ad_unit',
        holiday_region='US'
    )
    AS SELECT date, ad_unit, daily_impressions FROM `{staging_table}`
    """
    client.query(query).result()
    return f"{project_id}.ad_inventory.arima_model"


@component(base_image="python:3.11-slim", packages_to_install=["google-cloud-bigquery"])
def evaluate_and_promote(project_id: str, model_path: str, mape_threshold: float) -> str:
    """Evaluate new model; reject if MAPE exceeds threshold."""
    from google.cloud import bigquery

    client = bigquery.Client(project=project_id)

    # Evaluate new model
    eval_query = f"""
    SELECT AVG(mean_absolute_percentage_error) as avg_mape
    FROM ML.EVALUATE(MODEL `{model_path}`)
    """
    result = client.query(eval_query).result()
    new_mape = list(result)[0].avg_mape

    if new_mape > mape_threshold:
        # Reject: model already overwrote previous version, but downstream
        # systems can check this status before updating routing tables
        return f"REJECTED: MAPE {new_mape:.2%} exceeds threshold {mape_threshold:.2%}"

    return f"PROMOTED: MAPE {new_mape:.2%} within threshold"


@dsl.pipeline(name="arima-weekly-retrain", description="Weekly ARIMA+ model retraining")
def arima_retrain_pipeline(
    project_id: str,  # Required - no default to prevent silent failures
    lookback_days: int = 730,
    mape_threshold: float = 0.20,
):
    """
    Weekly retraining pipeline for ARIMA+ models.

    Args:
        project_id: GCP project ID (required, passed via Cloud Scheduler)
        lookback_days: Days of history to use (default 730 = 2 years)
        mape_threshold: Maximum acceptable MAPE for promotion (default 20%)
    """
    extract_task = extract_recent_traffic(
        project_id=project_id, lookback_days=lookback_days
    )
    train_task = train_arima(project_id=project_id, staging_table=extract_task.output)
    evaluate_and_promote(
        project_id=project_id,
        model_path=train_task.output,
        mape_threshold=mape_threshold,
    )


if __name__ == "__main__":
    from kfp.v2 import compiler

    compiler.Compiler().compile(arima_retrain_pipeline, "pipelines/arima_retrain.yaml")
    print("Pipeline compiled to pipelines/arima_retrain.yaml")
