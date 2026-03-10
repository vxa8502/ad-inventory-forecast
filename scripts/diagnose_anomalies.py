"""Diagnose Bitcoin and Influenza spot-check anomalies.

Investigates why these articles failed hypothesis validation:
- Bitcoin: Expected -15% weekend effect, got -7%
- Influenza: Expected d=0 (stationary), got d=1

Usage:
    python -m scripts.diagnose_anomalies
"""

from __future__ import annotations

import logging
import sys
from typing import Any

import pandas as pd

from config import settings
from config.helpers import safe_dataframe_extract, table_ref
from src.bq_client import get_client, query_to_dataframe
from src.cli import require_project_id
from src.logging_config import setup_logging
from src.pipeline_utils import PIPELINE_EXCEPTIONS
from src.printing_utils import print_section, print_summary_header
from src.validators import ensure_dataframe_has_data

# Pre-compute table reference for queries
_IMPRESSIONS_TABLE = table_ref("daily_impressions")

setup_logging()
logger = logging.getLogger(__name__)


def _extract_year_pair(
    df: pd.DataFrame,
    year_col: str,
    value_col: str,
    context: str,
) -> tuple[float | None, float | None, bool]:
    """Extract 2023 and 2024 values from a DataFrame.

    Args:
        df: DataFrame with year column.
        year_col: Name of the year column.
        value_col: Name of the value column to extract.
        context: Description for logging.

    Returns:
        Tuple of (value_2023, value_2024, is_valid).
        is_valid is False if either value is None.
    """
    val_2023 = safe_dataframe_extract(df, year_col, 2023, value_col)
    val_2024 = safe_dataframe_extract(df, year_col, 2024, value_col)

    if val_2023 is None or val_2024 is None:
        logger.warning("Missing year data for %s", context)
        return val_2023, val_2024, False

    return val_2023, val_2024, True


def _print_year_comparison(
    label_2023: str,
    val_2023: float,
    label_2024: str,
    val_2024: float,
    shift_label: str,
    format_spec: str = "+.1f",
    suffix: str = "%",
) -> float:
    """Print year-over-year comparison and return the shift.

    Args:
        label_2023: Label for 2023 value.
        val_2023: 2023 value.
        label_2024: Label for 2024 value.
        val_2024: 2024 value.
        shift_label: Label for the shift value.
        format_spec: Format specifier for values.
        suffix: Suffix for values (e.g., "%").

    Returns:
        The calculated shift (val_2024 - val_2023).
    """
    shift = val_2024 - val_2023
    print(f"\nAnalysis:")
    print(f"  {label_2023}: {val_2023:{format_spec}}{suffix}")
    print(f"  {label_2024}: {val_2024:{format_spec}}{suffix}")
    print(f"  {shift_label}: {shift:{format_spec}}{suffix}")
    return shift

__all__ = [
    "investigate_bitcoin_by_year",
    "investigate_bitcoin_vs_stock",
    "investigate_influenza_trend",
    "investigate_influenza_baseline",
]


def investigate_bitcoin_by_year() -> dict[str, Any]:
    """Investigation 1: Bitcoin weekend effect by year.

    Did the trading pattern shift from 2023 to 2024?
    """
    print_section("Investigation 1: Bitcoin Weekend Effect by Year")

    query = f"""
    WITH daily_stats AS (
        SELECT
            EXTRACT(YEAR FROM date) AS year,
            EXTRACT(DAYOFWEEK FROM date) AS dow,
            CASE WHEN EXTRACT(DAYOFWEEK FROM date) IN (1, 7) THEN 'Weekend' ELSE 'Weekday' END AS day_type,
            daily_impressions
        FROM {_IMPRESSIONS_TABLE}
        WHERE ad_unit = 'Bitcoin'
    ),
    yearly_averages AS (
        SELECT
            year,
            day_type,
            AVG(daily_impressions) AS avg_impressions,
            COUNT(*) AS days
        FROM daily_stats
        GROUP BY year, day_type
    ),
    pivoted AS (
        SELECT
            year,
            MAX(CASE WHEN day_type = 'Weekday' THEN avg_impressions END) AS weekday_avg,
            MAX(CASE WHEN day_type = 'Weekend' THEN avg_impressions END) AS weekend_avg
        FROM yearly_averages
        GROUP BY year
    )
    SELECT
        year,
        ROUND(weekday_avg, 0) AS weekday_avg,
        ROUND(weekend_avg, 0) AS weekend_avg,
        ROUND((weekend_avg - weekday_avg) / weekday_avg * 100, 1) AS weekend_effect_pct
    FROM pivoted
    ORDER BY year
    """

    df = query_to_dataframe(query, "Querying Bitcoin weekend effect by year")
    print("Bitcoin Weekend Effect by Year:")
    print(df.to_string(index=False))

    # Extract and compare year-over-year values
    effect_2023, effect_2024, is_valid = _extract_year_pair(
        df, "year", "weekend_effect_pct", "Bitcoin analysis"
    )

    if not is_valid:
        print("\nAnalysis: Insufficient data for year-over-year comparison")
        return {"query": "bitcoin_by_year", "data": df.to_dict()}

    shift = _print_year_comparison(
        "2023 weekend effect", effect_2023,
        "2024 weekend effect", effect_2024,
        "Year-over-year shift",
    )

    if abs(shift) > settings.YEAR_SHIFT_SIGNIFICANT_PCT:
        print("  Finding: Significant temporal shift detected")
    else:
        print("  Finding: Pattern relatively stable across years")

    return {"query": "bitcoin_by_year", "data": df.to_dict()}


def investigate_bitcoin_vs_stock() -> dict[str, Any]:
    """Investigation 2: Bitcoin vs Stock_market weekend effects.

    Compare crypto (24/7) vs traditional finance (market hours).
    """
    print_section("Investigation 2: Bitcoin vs Stock_market Comparison")

    query = f"""
    WITH daily_stats AS (
        SELECT
            ad_unit,
            CASE WHEN EXTRACT(DAYOFWEEK FROM date) IN (1, 7) THEN 'Weekend' ELSE 'Weekday' END AS day_type,
            daily_impressions
        FROM {_IMPRESSIONS_TABLE}
        WHERE ad_unit IN ('Bitcoin', 'Stock_market')
    ),
    averages AS (
        SELECT
            ad_unit,
            day_type,
            AVG(daily_impressions) AS avg_impressions
        FROM daily_stats
        GROUP BY ad_unit, day_type
    ),
    pivoted AS (
        SELECT
            ad_unit,
            MAX(CASE WHEN day_type = 'Weekday' THEN avg_impressions END) AS weekday_avg,
            MAX(CASE WHEN day_type = 'Weekend' THEN avg_impressions END) AS weekend_avg
        FROM averages
        GROUP BY ad_unit
    )
    SELECT
        ad_unit,
        ROUND(weekday_avg, 0) AS weekday_avg,
        ROUND(weekend_avg, 0) AS weekend_avg,
        ROUND((weekend_avg - weekday_avg) / weekday_avg * 100, 1) AS weekend_effect_pct,
        CASE
            WHEN (weekend_avg - weekday_avg) / weekday_avg < -0.15 THEN 'Strong weekday bias'
            WHEN (weekend_avg - weekday_avg) / weekday_avg < -0.05 THEN 'Moderate weekday bias'
            WHEN (weekend_avg - weekday_avg) / weekday_avg > 0.05 THEN 'Weekend increase'
            ELSE 'Relatively flat'
        END AS pattern_type
    FROM pivoted
    ORDER BY ad_unit
    """

    df = query_to_dataframe(query, "Querying Bitcoin vs Stock_market comparison")
    print("Weekend Effect Comparison:")
    print(df.to_string(index=False))

    # Analysis with safe extraction
    bitcoin_effect = safe_dataframe_extract(df, "ad_unit", "Bitcoin", "weekend_effect_pct")
    stock_effect = safe_dataframe_extract(df, "ad_unit", "Stock_market", "weekend_effect_pct")

    if bitcoin_effect is None or stock_effect is None:
        logger.warning("Missing ad_unit data for comparison")
        print("\nAnalysis: Insufficient data for comparison")
        return {"query": "bitcoin_vs_stock", "data": df.to_dict()}

    print(f"\nAnalysis:")
    print(f"  Bitcoin weekend effect: {bitcoin_effect:+.1f}%")
    print(f"  Stock_market weekend effect: {stock_effect:+.1f}%")
    print(f"  Difference: {abs(bitcoin_effect - stock_effect):.1f} percentage points")

    if stock_effect < settings.STOCK_WEEKEND_THRESHOLD_PCT and bitcoin_effect > settings.BITCOIN_WEEKEND_THRESHOLD_PCT:
        print("  Finding: Confirms hypothesis - Stock_market shows traditional")
        print("           market hours pattern while Bitcoin is more 24/7")

    return {"query": "bitcoin_vs_stock", "data": df.to_dict()}


def investigate_influenza_trend() -> dict[str, Any]:
    """Investigation 3: Influenza trend analysis.

    Is d=1 justified by examining raw daily traffic?
    """
    print_section("Investigation 3: Influenza Trend Analysis")

    # Monthly aggregation to see trend
    query = f"""
    WITH monthly AS (
        SELECT
            DATE_TRUNC(date, MONTH) AS month,
            AVG(daily_impressions) AS avg_daily,
            MIN(daily_impressions) AS min_daily,
            MAX(daily_impressions) AS max_daily
        FROM {_IMPRESSIONS_TABLE}
        WHERE ad_unit = 'Influenza'
        GROUP BY month
    ),
    with_lag AS (
        SELECT
            month,
            avg_daily,
            LAG(avg_daily) OVER (ORDER BY month) AS prev_month_avg,
            min_daily,
            max_daily
        FROM monthly
    )
    SELECT
        FORMAT_DATE('%Y-%m', month) AS month,
        ROUND(avg_daily, 0) AS avg_daily,
        ROUND(min_daily, 0) AS min_daily,
        ROUND(max_daily, 0) AS max_daily,
        ROUND((avg_daily - prev_month_avg) / NULLIF(prev_month_avg, 0) * 100, 1) AS mom_change_pct
    FROM with_lag
    ORDER BY month
    """

    df = query_to_dataframe(query, "Querying Influenza monthly trend")
    print("Influenza Monthly Traffic:")
    print(df.to_string(index=False))

    if not ensure_dataframe_has_data(df, "Influenza trend"):
        print("\nTrend Analysis: Insufficient data")
        return {"query": "influenza_trend", "data": {}}

    # Calculate overall trend
    first_val = df.iloc[0]["avg_daily"]
    last_val = df.iloc[-1]["avg_daily"]
    overall_change = (last_val - first_val) / first_val * 100 if first_val else 0

    # Calculate variance
    mean_val = df["avg_daily"].mean()
    variance = df["avg_daily"].std() / mean_val * 100 if mean_val else 0

    print(f"\nTrend Analysis:")
    print(f"  First month avg: {first_val:,.0f}")
    print(f"  Last month avg: {last_val:,.0f}")
    print(f"  Overall change: {overall_change:+.1f}%")
    print(f"  Coefficient of variation: {variance:.1f}%")

    # Check for flu season pattern
    df["month_num"] = pd.to_datetime(df["month"]).dt.month
    flu_season = df[df["month_num"].isin([10, 11, 12, 1, 2, 3])]["avg_daily"].mean()
    non_flu_season = df[df["month_num"].isin([4, 5, 6, 7, 8, 9])]["avg_daily"].mean()

    print(f"\nSeasonality Check:")
    print(f"  Flu season avg (Oct-Mar): {flu_season:,.0f}")
    print(f"  Off-season avg (Apr-Sep): {non_flu_season:,.0f}")
    if non_flu_season and non_flu_season > 0 and pd.notna(non_flu_season):
        print(f"  Seasonal ratio: {flu_season / non_flu_season:.2f}x")

    if abs(overall_change) > settings.TREND_SIGNIFICANT_PCT:
        print(f"\n  Finding: Strong overall trend ({overall_change:+.1f}%) justifies d=1")
    else:
        print(f"\n  Finding: d=1 may be driven by within-season trends, not annual drift")

    return {"query": "influenza_trend", "data": df.to_dict()}


def investigate_influenza_baseline() -> dict[str, Any]:
    """Investigation 4: Influenza 2023 vs 2024 baseline.

    Detect post-COVID structural shift.
    """
    print_section("Investigation 4: Influenza 2023 vs 2024 Baseline")

    query = f"""
    WITH yearly_stats AS (
        SELECT
            EXTRACT(YEAR FROM date) AS year,
            AVG(daily_impressions) AS avg_daily,
            STDDEV(daily_impressions) AS stddev_daily,
            MIN(daily_impressions) AS min_daily,
            MAX(daily_impressions) AS max_daily,
            COUNT(*) AS days
        FROM {_IMPRESSIONS_TABLE}
        WHERE ad_unit = 'Influenza'
        GROUP BY year
    )
    SELECT
        year,
        ROUND(avg_daily, 0) AS avg_daily,
        ROUND(stddev_daily, 0) AS stddev,
        ROUND(stddev_daily / avg_daily, 2) AS cv,
        ROUND(min_daily, 0) AS min_daily,
        ROUND(max_daily, 0) AS max_daily,
        days
    FROM yearly_stats
    ORDER BY year
    """

    df = query_to_dataframe(query, "Querying Influenza yearly comparison")
    print("Influenza Yearly Comparison:")
    print(df.to_string(index=False))

    # Extract and compare year-over-year values
    avg_2023, avg_2024, is_valid = _extract_year_pair(
        df, "year", "avg_daily", "Influenza baseline analysis"
    )

    if not is_valid:
        print("\nBaseline Analysis: Insufficient data for year-over-year comparison")
    else:
        baseline_shift = (avg_2024 - avg_2023) / avg_2023 * 100

        print(f"\nBaseline Analysis:")
        print(f"  2023 average: {avg_2023:,.0f}")
        print(f"  2024 average: {avg_2024:,.0f}")
        print(f"  Year-over-year shift: {baseline_shift:+.1f}%")

        if abs(baseline_shift) > settings.BASELINE_SHIFT_SIGNIFICANT_PCT:
            print(f"\n  Finding: Significant baseline shift ({baseline_shift:+.1f}%)")
            print("           This structural change justifies d=1 differencing")
        else:
            print("\n  Finding: Baseline relatively stable")
            print("           d=1 may be due to within-year seasonality trends")

    # Compare same months across years
    query_monthly = f"""
    SELECT
        EXTRACT(MONTH FROM date) AS month,
        AVG(CASE WHEN EXTRACT(YEAR FROM date) = 2023 THEN daily_impressions END) AS avg_2023,
        AVG(CASE WHEN EXTRACT(YEAR FROM date) = 2024 THEN daily_impressions END) AS avg_2024
    FROM {_IMPRESSIONS_TABLE}
    WHERE ad_unit = 'Influenza'
    GROUP BY month
    ORDER BY month
    """

    df_monthly = query_to_dataframe(query_monthly, "Querying month-over-month comparison")
    df_monthly['yoy_change'] = ((df_monthly['avg_2024'] - df_monthly['avg_2023'])
                                 / df_monthly['avg_2023'] * 100).round(1)

    print(f"\nMonth-over-Month Comparison (2023 vs 2024):")
    print(df_monthly.to_string(index=False))

    return {"query": "influenza_baseline", "data": df.to_dict()}


def main() -> None:
    """Run all diagnostic investigations."""
    require_project_id()

    print_summary_header("ANOMALY DIAGNOSIS: Bitcoin & Influenza")
    print("  Investigating spot-check validation failures\n")

    # Initialize client (validates connection)
    get_client()

    try:
        # Investigation 1: Bitcoin by year
        investigate_bitcoin_by_year()

        # Investigation 2: Bitcoin vs Stock_market
        investigate_bitcoin_vs_stock()

        # Investigation 3: Influenza trend
        investigate_influenza_trend()

        # Investigation 4: Influenza baseline
        investigate_influenza_baseline()

        # Final summary
        print_section("SUMMARY: Root Cause Analysis")

        print("BITCOIN (-7% weekend effect, expected -15%):")
        print("  1. Compare years to see if pattern shifted")
        print("  2. Compare to Stock_market to validate crypto is truly 24/7")
        print("  3. Hypothesis was based on traditional trading hours")
        print("  4. Modern crypto retail trading is global, 24/7")

        print("\nINFLUENZA (got d=1, expected d=0):")
        print("  1. Check monthly trend for overall direction")
        print("  2. Compare 2023 vs 2024 baselines for structural shift")
        print("  3. 92-day forecast window cannot capture yearly seasonality")
        print("  4. Post-COVID interest patterns may differ from historical")

        print_section("Diagnosis complete. Review findings above.")

    except PIPELINE_EXCEPTIONS as e:
        logger.exception("Diagnosis failed: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
