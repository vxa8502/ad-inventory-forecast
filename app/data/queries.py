"""Cached BigQuery query functions for the dashboard."""

from __future__ import annotations

__all__ = [
    # Decorator
    "cached_query",
    # Data fetching
    "get_actuals",
    "get_ad_units",
    "get_forecasts",
    "get_model_comparison",
    "get_metrics_detail",
    "get_business_impact",
    "get_decomposition",
    "get_anomalies",
    "get_all_anomalies_for_ad_unit",
    "get_volatility_metrics",
    "get_anomalies_for_chart",
    "get_residuals",
    "get_holidays_in_range",
    "get_holiday_effects",
    "get_step_change_articles",
]

import logging
from functools import wraps
from pathlib import Path
from typing import Any, Callable

import pandas as pd
import streamlit as st
from google.api_core import exceptions as gcp_exceptions
from google.cloud import bigquery

from config.helpers import table_ref
from app.constants import QUERY_CACHE_TTL, FOLD_CROSS_AVG, FOLD_1, FOLD_2
from app.utils.state import get_bq_client


def _build_query_config(
    params: list[bigquery.ScalarQueryParameter | bigquery.ArrayQueryParameter],
) -> bigquery.QueryJobConfig:
    """Build QueryJobConfig with parameterized query parameters.

    Parameterized queries prevent SQL injection attacks by separating
    query structure from user-provided values. BigQuery validates and
    escapes parameters server-side.

    Args:
        params: List of BigQuery scalar or array parameters.

    Returns:
        QueryJobConfig ready for client.query(..., job_config=...).
    """
    return bigquery.QueryJobConfig(query_parameters=params)


def _param(
    name: str,
    bq_type: str,
    value: Any,
) -> bigquery.ScalarQueryParameter | bigquery.ArrayQueryParameter:
    """Build BigQuery parameter (scalar or array) based on value type.

    Unified factory that eliminates separate scalar/array builders.
    Arrays are automatically sorted for consistent cache keys.

    Args:
        name: Parameter name (matches @name in query).
        bq_type: BigQuery type string (STRING, DATE, FLOAT64, etc.).
        value: Parameter value (list/tuple for array, scalar otherwise).

    Returns:
        ScalarQueryParameter or ArrayQueryParameter ready for query parameterization.
    """
    if isinstance(value, (list, tuple)):
        return bigquery.ArrayQueryParameter(name, bq_type, sorted(value))
    return bigquery.ScalarQueryParameter(name, bq_type, value)


def _execute_query(
    query: str,
    params: list[bigquery.ScalarQueryParameter | bigquery.ArrayQueryParameter] | None = None,
) -> pd.DataFrame:
    """Execute a parameterized query and return DataFrame.

    Centralizes the boilerplate execution pattern across all query functions:
    get client -> build config -> execute -> convert to DataFrame.

    Args:
        query: SQL query string with @parameter placeholders.
        params: List of BigQuery parameters. None for queries without parameters.

    Returns:
        Query results as a DataFrame.
    """
    client = get_bq_client()
    if params:
        return client.query(query, job_config=_build_query_config(params)).to_dataframe()
    return client.query(query).to_dataframe()


def _build_date_params(
    start_date: str | None,
    end_date: str | None,
) -> tuple[list[bigquery.ScalarQueryParameter], str]:
    """Build date range parameters and SQL filter clause.

    Args:
        start_date: Optional start date (YYYY-MM-DD).
        end_date: Optional end date (YYYY-MM-DD).

    Returns:
        Tuple of (params_list, filter_clause) where filter_clause is empty
        string or "AND date >= @start_date AND date <= @end_date" etc.
    """
    params: list[bigquery.ScalarQueryParameter] = []
    conditions: list[str] = []

    if start_date:
        conditions.append("date >= @start_date")
        params.append(_param("start_date", "DATE", start_date))
    if end_date:
        conditions.append("date <= @end_date")
        params.append(_param("end_date", "DATE", end_date))

    filter_clause = ""
    if conditions:
        filter_clause = " AND " + " AND ".join(conditions)

    return params, filter_clause


logger = logging.getLogger(__name__)

# Valid fold names for parameter validation
VALID_FOLDS: frozenset[str] = frozenset({FOLD_1, FOLD_2, FOLD_CROSS_AVG})


def _validate_fold(fold: str) -> bool:
    """Validate fold name is one of the expected values.

    Args:
        fold: Fold name to validate.

    Returns:
        True if valid, False otherwise.
    """
    if fold not in VALID_FOLDS:
        logger.warning(f"Invalid fold name: '{fold}'. Expected one of {VALID_FOLDS}")
        return False
    return True


def _handle_query_error(func: Callable[..., pd.DataFrame]) -> Callable[..., pd.DataFrame]:
    """Decorator to handle query errors gracefully.

    Logs full traceback for debugging, displays user-friendly message,
    and returns empty DataFrame on failure. Catches specific Google Cloud
    exceptions for better error categorization.

    Args:
        func: Query function to wrap.

    Returns:
        Wrapped function that handles errors gracefully.
    """
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> pd.DataFrame:
        try:
            return func(*args, **kwargs)
        except gcp_exceptions.NotFound as e:
            logger.exception(f"NotFound in {func.__name__}: {e}")
            st.error("Data not found: Table or resource not accessible")
            return pd.DataFrame()
        except gcp_exceptions.BadRequest as e:
            logger.exception(f"BadRequest in {func.__name__}: {e}")
            st.error("Query error: Invalid request. Check query parameters.")
            return pd.DataFrame()
        except gcp_exceptions.Forbidden as e:
            logger.exception(f"Forbidden in {func.__name__}: {e}")
            st.error("Access denied: Insufficient permissions")
            return pd.DataFrame()
        except gcp_exceptions.GoogleAPIError as e:
            logger.exception(f"GoogleAPIError in {func.__name__}: {type(e).__name__}: {e}")
            st.error("API error: Unable to complete request. Please try again.")
            return pd.DataFrame()
        except KeyError as e:
            logger.exception(f"KeyError in {func.__name__}: missing column {e}")
            st.error("Data structure error: Expected column not found")
            return pd.DataFrame()
        except (ValueError, TypeError) as e:
            logger.exception(f"Data error in {func.__name__}: {type(e).__name__}: {e}")
            st.error("Invalid data: Unable to process query results")
            return pd.DataFrame()
    return wrapper


def cached_query(spinner_text: str) -> Callable[[Callable[..., pd.DataFrame]], Callable[..., pd.DataFrame]]:
    """Decorator factory combining error handling and Streamlit caching.

    Eliminates repeated decorator stacks across query functions by
    combining _handle_query_error and st.cache_data into a single decorator.

    Args:
        spinner_text: Text shown in Streamlit spinner during query execution.

    Returns:
        Decorator that wraps query functions with error handling and caching.

    Example:
        @cached_query("Loading forecasts...")
        def get_forecasts(...) -> pd.DataFrame:
            ...
    """
    def decorator(func: Callable[..., pd.DataFrame]) -> Callable[..., pd.DataFrame]:
        cached_func = st.cache_data(ttl=QUERY_CACHE_TTL, show_spinner=spinner_text)(func)
        return _handle_query_error(cached_func)
    return decorator


@cached_query("Loading actuals...")
def get_actuals(
    ad_units: list[str],
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """Fetch actual daily impressions for selected ad units.

    Args:
        ad_units: List of ad unit names to fetch.
        start_date: Optional start date filter (YYYY-MM-DD).
        end_date: Optional end date filter (YYYY-MM-DD).

    Returns:
        DataFrame with columns: date, ad_unit, daily_impressions
    """
    if not ad_units:
        return pd.DataFrame()

    date_params, date_filter = _build_date_params(start_date, end_date)
    params: list[bigquery.ScalarQueryParameter | bigquery.ArrayQueryParameter] = [
        _param("ad_units", "STRING", ad_units),
        *date_params,
    ]

    query = f"""
    SELECT
        date,
        ad_unit,
        daily_impressions
    FROM {table_ref('daily_impressions')}
    WHERE ad_unit IN UNNEST(@ad_units)
    {date_filter}
    ORDER BY ad_unit, date
    """

    return _execute_query(query, params)


@cached_query("Loading ad units...")
def get_ad_units() -> list[str]:
    """Fetch distinct ad units from daily_impressions table.

    Returns:
        List of ad unit names.
    """
    query = f"""
    SELECT DISTINCT ad_unit
    FROM {table_ref('daily_impressions')}
    ORDER BY ad_unit
    """

    return _execute_query(query)["ad_unit"].tolist()


@cached_query("Loading forecasts...")
def get_forecasts(
    ad_units: list[str],
    models: list[str],
    fold: str,
) -> pd.DataFrame:
    """Fetch forecasts for selected ad units and models.

    Args:
        ad_units: List of ad unit names.
        models: List of model names (e.g., ['timesfm_2_5', 'arima_plus']).
        fold: Fold name (fold_1, fold_2).

    Returns:
        DataFrame with columns: date, ad_unit, model_name, forecast,
        lower_bound, upper_bound
    """
    if not all([ad_units, models, _validate_fold(fold)]):
        return pd.DataFrame()

    params = [
        _param("ad_units", "STRING", ad_units),
        _param("models", "STRING", models),
        _param("fold", "STRING", fold),
    ]

    query = f"""
    SELECT
        forecast_date AS date,
        ad_unit,
        model_name,
        forecast_value AS forecast,
        forecast_lower AS lower_bound,
        forecast_upper AS upper_bound
    FROM {table_ref('forecasts')}
    WHERE ad_unit IN UNNEST(@ad_units)
      AND model_name IN UNNEST(@models)
      AND fold_name = @fold
    ORDER BY model_name, ad_unit, forecast_date
    """

    return _execute_query(query, params)


@cached_query("Loading model comparison...")
def get_model_comparison(fold: str = FOLD_CROSS_AVG) -> pd.DataFrame:
    """Fetch model comparison metrics.

    Args:
        fold: Fold name (FOLD_CROSS_AVG, FOLD_1, FOLD_2).

    Returns:
        DataFrame with columns: model_name, metric_name, metric_value
    """
    if not _validate_fold(fold):
        return pd.DataFrame()

    params = [_param("fold", "STRING", fold)]

    query = f"""
    SELECT
        model_name,
        metric_name,
        mean_value AS metric_value
    FROM {table_ref('model_comparison')}
    WHERE fold_name = @fold
    ORDER BY model_name, metric_name
    """

    return _execute_query(query, params)


@cached_query("Loading detailed metrics...")
def get_metrics_detail(fold: str) -> pd.DataFrame:
    """Fetch detailed per-ad-unit metrics.

    Args:
        fold: Fold name (fold_1, fold_2).

    Returns:
        DataFrame with columns: model_name, ad_unit, metric_name, metric_value
    """
    params = [_param("fold", "STRING", fold)]

    query = f"""
    SELECT
        model_name,
        ad_unit,
        metric_name,
        metric_value
    FROM {table_ref('model_metrics')}
    WHERE fold_name = @fold
    ORDER BY model_name, ad_unit, metric_name
    """

    return _execute_query(query, params)


@cached_query("Loading business impact...")
def get_business_impact(fold: str = FOLD_CROSS_AVG) -> pd.DataFrame:
    """Fetch business impact analysis.

    Args:
        fold: Fold name (FOLD_CROSS_AVG, FOLD_1, FOLD_2).

    Returns:
        DataFrame with columns: model_name, ad_unit, avg_daily_impressions,
        mape, cpm, daily_revenue_at_risk, annual_revenue_at_risk
    """
    if not _validate_fold(fold):
        return pd.DataFrame()

    params = [_param("fold", "STRING", fold)]

    query = f"""
    SELECT
        model_name,
        ad_unit,
        avg_daily_impressions,
        mape,
        cpm,
        daily_revenue_at_risk,
        annual_revenue_at_risk
    FROM {table_ref('business_impact')}
    WHERE fold_name = @fold
    ORDER BY annual_revenue_at_risk DESC
    """

    return _execute_query(query, params)


@cached_query("Loading decomposition...")
def get_decomposition(ad_unit: str, fold: str) -> pd.DataFrame:
    """Fetch ARIMA decomposition for a single ad unit.

    Args:
        ad_unit: Ad unit name.
        fold: Fold name (fold_1, fold_2).

    Returns:
        DataFrame with columns: date, trend, seasonal_weekly,
        seasonal_yearly, holiday_effect, residual
    """
    if not all([ad_unit, _validate_fold(fold)]):
        return pd.DataFrame()

    params = [
        _param("ad_unit", "STRING", ad_unit),
        _param("fold", "STRING", fold),
    ]

    query = f"""
    SELECT
        forecast_date AS date,
        trend,
        seasonal_period_weekly AS seasonal_weekly,
        seasonal_period_yearly AS seasonal_yearly,
        holiday_effect,
        step_changes AS step_change
    FROM {table_ref('forecast_decomposition')}
    WHERE ad_unit = @ad_unit
      AND fold_name = @fold
    ORDER BY forecast_date
    """

    return _execute_query(query, params)


@cached_query("Loading anomalies...")
def get_anomalies(
    ad_units: list[str],
    fold: str,
    threshold: float = 0.95,
) -> pd.DataFrame:
    """Fetch detected anomalies for selected ad units.

    Args:
        ad_units: List of ad unit names.
        fold: Fold name (fold_1, fold_2).
        threshold: Minimum anomaly probability threshold.

    Returns:
        DataFrame with columns: date, ad_unit, daily_impressions,
        is_anomaly, lower_bound, upper_bound, anomaly_probability
    """
    if not all([ad_units, _validate_fold(fold)]):
        return pd.DataFrame()

    params = [
        _param("ad_units", "STRING", ad_units),
        _param("fold", "STRING", fold),
        _param("threshold", "FLOAT64", threshold),
    ]

    query = f"""
    SELECT
        date,
        ad_unit,
        daily_impressions,
        is_anomaly,
        lower_bound,
        upper_bound,
        anomaly_probability
    FROM {table_ref('anomalies')}
    WHERE ad_unit IN UNNEST(@ad_units)
      AND fold_name = @fold
      AND (anomaly_probability >= @threshold OR is_anomaly = TRUE)
    ORDER BY ad_unit, date
    """

    return _execute_query(query, params)


@cached_query("Loading all anomalies...")
def get_all_anomalies_for_ad_unit(ad_unit: str, fold: str) -> pd.DataFrame:
    """Fetch all data points (anomaly and non-anomaly) for charting.

    Args:
        ad_unit: Ad unit name.
        fold: Fold name (fold_1, fold_2).

    Returns:
        DataFrame with all time series points and anomaly flags.
    """
    params = [
        _param("ad_unit", "STRING", ad_unit),
        _param("fold", "STRING", fold),
    ]

    query = f"""
    SELECT
        date,
        ad_unit,
        daily_impressions,
        is_anomaly,
        lower_bound,
        upper_bound,
        anomaly_probability
    FROM {table_ref('anomalies')}
    WHERE ad_unit = @ad_unit
      AND fold_name = @fold
    ORDER BY date
    """

    return _execute_query(query, params)


@cached_query("Loading volatility metrics...")
def get_volatility_metrics() -> pd.DataFrame:
    """Fetch coefficient of variation (CV) for each ad unit.

    CV = stddev / mean. Higher CV indicates more volatile, event-driven content.
    Used for model routing recommendations (TimesFM for CV > 0.5).

    Returns:
        DataFrame with columns: ad_unit, cv (coefficient of variation)
    """
    query = f"""
    SELECT
        ad_unit,
        SAFE_DIVIDE(STDDEV(daily_impressions), AVG(daily_impressions)) AS cv
    FROM {table_ref('daily_impressions')}
    GROUP BY ad_unit
    ORDER BY cv DESC
    """

    return _execute_query(query)


@cached_query("Loading anomalies for chart...")
def get_anomalies_for_chart(
    ad_unit: str,
    fold: str,
    threshold: float = 0.95,
) -> pd.DataFrame:
    """Fetch anomalies for a single ad unit to overlay on forecast chart.

    Args:
        ad_unit: Ad unit name.
        fold: Fold name (fold_1, fold_2).
        threshold: Minimum anomaly probability threshold.

    Returns:
        DataFrame with columns: date, anomaly_probability
    """
    params = [
        _param("ad_unit", "STRING", ad_unit),
        _param("fold", "STRING", fold),
        _param("threshold", "FLOAT64", threshold),
    ]

    query = f"""
    SELECT
        date,
        anomaly_probability
    FROM {table_ref('anomalies')}
    WHERE ad_unit = @ad_unit
      AND fold_name = @fold
      AND (anomaly_probability >= @threshold OR is_anomaly = TRUE)
    ORDER BY date
    """

    return _execute_query(query, params)


@cached_query("Loading residuals...")
def get_residuals(fold: str) -> pd.DataFrame:
    """Fetch forecast residuals (forecast - actual) for all models.

    Residuals show the direction and magnitude of forecast errors.
    Positive = overforecast, negative = underforecast.

    Args:
        fold: Fold name (fold_1, fold_2).

    Returns:
        DataFrame with columns: model_name, ad_unit, date, residual
    """
    params = [_param("fold", "STRING", fold)]

    query = f"""
    SELECT
        f.model_name,
        f.ad_unit,
        f.forecast_date AS date,
        f.forecast_value - a.daily_impressions AS residual
    FROM {table_ref('forecasts')} f
    INNER JOIN {table_ref('daily_impressions')} a
        ON f.ad_unit = a.ad_unit
        AND f.forecast_date = a.date
    WHERE f.fold_name = @fold
    ORDER BY f.model_name, f.ad_unit, f.forecast_date
    """

    return _execute_query(query, params)


@cached_query("Loading holidays...")
def get_holidays_in_range(start_date: str, end_date: str) -> pd.DataFrame:
    """Fetch US holidays within a date range.

    Loads holidays from the reference CSV for overlay on decomposition charts.

    Args:
        start_date: Start date (YYYY-MM-DD).
        end_date: End date (YYYY-MM-DD).

    Returns:
        DataFrame with columns: holiday_date, holiday_name, is_major
    """
    holidays_path = Path(__file__).parent.parent.parent / "data" / "reference" / "us_holidays.csv"

    if not holidays_path.exists():
        return pd.DataFrame()

    df = pd.read_csv(holidays_path)
    df["holiday_date"] = pd.to_datetime(df["holiday_date"])
    # Convert is_major to boolean (handles both string "true"/"false" and bool)
    if df["is_major"].dtype == "object":
        df["is_major"] = df["is_major"].str.lower() == "true"
    else:
        df["is_major"] = df["is_major"].astype(bool)

    # Filter to date range
    mask = (df["holiday_date"] >= start_date) & (df["holiday_date"] <= end_date)
    return df[mask].copy()


@cached_query("Loading holiday effects...")
def get_holiday_effects(ad_unit: str, fold: str) -> pd.DataFrame:
    """Fetch dates with significant holiday effects for an ad unit.

    Args:
        ad_unit: Ad unit name.
        fold: Fold name (fold_1, fold_2).

    Returns:
        DataFrame with columns: date, holiday_effect (non-zero values only)
    """
    params = [
        _param("ad_unit", "STRING", ad_unit),
        _param("fold", "STRING", fold),
    ]

    query = f"""
    SELECT
        forecast_date AS date,
        holiday_effect
    FROM {table_ref('forecast_decomposition')}
    WHERE ad_unit = @ad_unit
      AND fold_name = @fold
      AND ABS(holiday_effect) > 0
    ORDER BY forecast_date
    """

    return _execute_query(query, params)


@cached_query("Loading step change info...")
def get_step_change_articles(fold: str) -> list[str]:
    """Fetch articles that have step changes detected.

    Args:
        fold: Fold name (fold_1, fold_2).

    Returns:
        List of ad unit names with step changes.
    """
    params = [_param("fold", "STRING", fold)]

    query = f"""
    SELECT DISTINCT ad_unit
    FROM {table_ref('arima_evaluate_' + fold)}
    WHERE has_step_changes = TRUE
    ORDER BY ad_unit
    """

    try:
        df = _execute_query(query, params)
        return df["ad_unit"].tolist() if not df.empty else []
    except gcp_exceptions.NotFound:
        # Table doesn't exist in this dataset - normal for some configurations
        logger.debug(f"Step change evaluation table not found for {fold}")
        return []
    except gcp_exceptions.GoogleAPIError as e:
        logger.warning(f"Error fetching step change articles: {type(e).__name__}: {e}")
        return []
