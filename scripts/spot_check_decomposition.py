"""Spot-check ARIMA_PLUS decomposition for plausibility.

Validates that trend, seasonal, and holiday components match expectations
from pre-training hypotheses (see README.md Model Analysis section).

Usage:
    python -m scripts.spot_check_decomposition [--fold FOLD]
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import Any

from google.cloud import bigquery

from config import settings
from config.helpers import table_ref
from src.bq_client import get_client
from src.cli import require_project_id
from src.logging_config import setup_logging
from src.pipeline_utils import PIPELINE_EXCEPTIONS
from src.printing_utils import (
    SEPARATOR_WIDTH,
    STATUS_ICONS,
    print_section,
    print_summary_header,
)

setup_logging()
logger = logging.getLogger(__name__)

__all__ = [
    "SPOT_CHECK_ARTICLES",
    "check_article",
    "run_spot_checks",
]

SPOT_CHECK_ARTICLES = {
    "Python_(programming_language)": {
        "vertical": "Technology",
        "expected_weekly": "Weekend dips (developers work M-F)",
        "expected_annual": "Mild summer dip",
        "expected_holiday": "Low sensitivity",
        "validation": lambda d: d["weekend_effect"] < -settings.WEEKEND_EFFECT_THRESHOLD,
        "validation_desc": f"Weekend effect < -{settings.WEEKEND_EFFECT_THRESHOLD:.0%}",
    },
    "NFL": {
        "vertical": "Sports",
        "expected_weekly": "Sunday/Monday spikes",
        "expected_annual": "September-February elevation",
        "expected_holiday": "Super Bowl Sunday",
        "validation": lambda d: d["weekly_amplitude"] > settings.WEEKLY_AMPLITUDE_THRESHOLD,
        "validation_desc": f"Weekly amplitude > {settings.WEEKLY_AMPLITUDE_THRESHOLD:.0%}",
    },
    "Bitcoin": {
        "vertical": "Finance",
        "expected_weekly": "Strong weekday pattern",
        "expected_annual": "Halving/ETF events",
        "expected_holiday": "Low (24/7 trading)",
        "validation": lambda d: d["weekend_effect"] < -settings.WEEKEND_EFFECT_STRONG_THRESHOLD,
        "validation_desc": f"Weekend effect < -{settings.WEEKEND_EFFECT_STRONG_THRESHOLD:.0%}",
    },
    "Influenza": {
        "vertical": "Health",
        "expected_weekly": "Flat",
        "expected_annual": "October-March flu season",
        "expected_holiday": "Low",
        "validation": lambda d: d["yearly_amplitude"] > settings.YEARLY_AMPLITUDE_THRESHOLD,
        "validation_desc": f"Yearly amplitude > {settings.YEARLY_AMPLITUDE_THRESHOLD:.0%}",
    },
    "Taylor_Swift": {
        "vertical": "Entertainment",
        "expected_weekly": "Slight weekend increase",
        "expected_annual": "Tour/album spikes",
        "expected_holiday": "Low",
        "validation": lambda d: True,
        "validation_desc": "Manual inspection (event-driven)",
    },
}


DECOMPOSITION_QUERY = """
WITH daily_components AS (
    SELECT
        ad_unit,
        forecast_date,
        forecast_value,
        trend,
        seasonal_period_weekly,
        seasonal_period_yearly,
        holiday_effect,
        -- Day of week (1=Sunday, 7=Saturday)
        EXTRACT(DAYOFWEEK FROM forecast_date) AS day_of_week
    FROM {table_ref}
    WHERE fold_name = '{fold_name}'
      AND ad_unit = '{ad_unit}'
),
weekly_stats AS (
    SELECT
        ad_unit,
        -- Weekend effect: avg weekend component vs weekday
        AVG(CASE WHEN day_of_week IN (1, 7) THEN seasonal_period_weekly END) AS weekend_weekly,
        AVG(CASE WHEN day_of_week NOT IN (1, 7) THEN seasonal_period_weekly END) AS weekday_weekly,
        -- Weekly amplitude (max - min)
        MAX(seasonal_period_weekly) - MIN(seasonal_period_weekly) AS weekly_range,
        AVG(forecast_value) AS avg_forecast
    FROM daily_components
    GROUP BY ad_unit
),
yearly_stats AS (
    SELECT
        ad_unit,
        MAX(seasonal_period_yearly) - MIN(seasonal_period_yearly) AS yearly_range,
        AVG(ABS(holiday_effect)) AS avg_holiday_effect,
        MAX(ABS(holiday_effect)) AS max_holiday_effect
    FROM daily_components
    GROUP BY ad_unit
)
SELECT
    w.ad_unit,
    -- Weekly pattern
    ROUND(w.weekend_weekly, 2) AS weekend_component,
    ROUND(w.weekday_weekly, 2) AS weekday_component,
    ROUND(SAFE_DIVIDE(w.weekend_weekly - w.weekday_weekly, w.avg_forecast), 3) AS weekend_effect,
    ROUND(SAFE_DIVIDE(w.weekly_range, w.avg_forecast), 3) AS weekly_amplitude,
    -- Yearly pattern
    ROUND(SAFE_DIVIDE(y.yearly_range, w.avg_forecast), 3) AS yearly_amplitude,
    -- Holiday pattern
    ROUND(y.avg_holiday_effect, 2) AS avg_holiday_effect,
    ROUND(y.max_holiday_effect, 2) AS max_holiday_effect,
    ROUND(SAFE_DIVIDE(y.max_holiday_effect, w.avg_forecast), 3) AS holiday_impact_pct,
    -- Baseline
    ROUND(w.avg_forecast, 0) AS avg_forecast
FROM weekly_stats w
JOIN yearly_stats y ON w.ad_unit = y.ad_unit
"""


ARIMA_ORDER_QUERY = """
SELECT
    ad_unit,
    arima_order,
    non_seasonal_d AS differencing,
    has_drift,
    ROUND(AIC, 2) AS aic,
    has_holiday_effect,
    has_step_changes
FROM {table_ref}
WHERE ad_unit = '{ad_unit}'
"""


def _fetch_arima_order(
    client: bigquery.Client,
    ad_unit: str,
    fold_name: str,
) -> tuple[dict[str, Any] | None, list[str]]:
    """Fetch ARIMA order information for an article.

    Args:
        client: BigQuery client.
        ad_unit: Article name.
        fold_name: Validation fold.

    Returns:
        Tuple of (order dict or None, list of notes/warnings).
    """
    notes: list[str] = []
    order_query = ARIMA_ORDER_QUERY.format(
        table_ref=table_ref(f"arima_evaluate_{fold_name}"),
        ad_unit=ad_unit,
    )

    try:
        order_df = client.query(order_query).to_dataframe()
        if not order_df.empty:
            row = order_df.iloc[0]
            return {
                "order": row["arima_order"],
                "differencing": int(row["differencing"]),
                "has_drift": row["has_drift"],
                "aic": row["aic"],
                "has_holiday_effect": row["has_holiday_effect"],
                "has_step_changes": row["has_step_changes"],
            }, notes
    except (KeyError, TypeError, RuntimeError) as e:
        notes.append(f"ARIMA order query failed: {e}")
        logger.warning("ARIMA order query failed for %s: %s", ad_unit, e)

    return None, notes


def _fetch_decomposition(
    client: bigquery.Client,
    ad_unit: str,
    fold_name: str,
) -> tuple[dict[str, Any] | None, list[str]]:
    """Fetch decomposition statistics for an article.

    Args:
        client: BigQuery client.
        ad_unit: Article name.
        fold_name: Validation fold.

    Returns:
        Tuple of (decomposition dict or None, list of notes/warnings).
    """
    notes: list[str] = []
    decomp_query = DECOMPOSITION_QUERY.format(
        table_ref=table_ref("forecast_decomposition"),
        fold_name=fold_name,
        ad_unit=ad_unit,
    )

    try:
        decomp_df = client.query(decomp_query).to_dataframe()
        if not decomp_df.empty:
            row = decomp_df.iloc[0]
            return {
                "weekend_effect": float(row["weekend_effect"]) if row["weekend_effect"] else 0,
                "weekly_amplitude": float(row["weekly_amplitude"]) if row["weekly_amplitude"] else 0,
                "yearly_amplitude": float(row["yearly_amplitude"]) if row["yearly_amplitude"] else 0,
                "avg_holiday_effect": float(row["avg_holiday_effect"]) if row["avg_holiday_effect"] else 0,
                "holiday_impact_pct": float(row["holiday_impact_pct"]) if row["holiday_impact_pct"] else 0,
                "avg_forecast": float(row["avg_forecast"]) if row["avg_forecast"] else 0,
            }, notes
        notes.append("No decomposition data found")
    except (KeyError, TypeError, RuntimeError) as e:
        notes.append(f"Decomposition query failed: {e}")
        logger.warning("Decomposition query failed for %s: %s", ad_unit, e)

    return None, notes


def check_article(
    client: bigquery.Client,
    ad_unit: str,
    fold_name: str,
    config: dict[str, Any],
) -> dict[str, Any]:
    """Run decomposition check for a single article.

    Args:
        client: BigQuery client.
        ad_unit: Article name.
        fold_name: Validation fold.
        config: Expected patterns from SPOT_CHECK_ARTICLES.

    Returns:
        Dictionary with check results.
    """
    result: dict[str, Any] = {
        "ad_unit": ad_unit,
        "vertical": config["vertical"],
        "fold": fold_name,
        "status": "UNKNOWN",
        "arima_order": None,
        "decomposition": None,
        "validation_passed": None,
        "notes": [],
    }

    # Fetch ARIMA order
    arima_order, arima_notes = _fetch_arima_order(client, ad_unit, fold_name)
    result["arima_order"] = arima_order
    result["notes"].extend(arima_notes)

    # Fetch decomposition
    decomp, decomp_notes = _fetch_decomposition(client, ad_unit, fold_name)
    result["decomposition"] = decomp
    result["notes"].extend(decomp_notes)

    # Determine status based on decomposition availability and validation
    if decomp is None:
        result["status"] = "MISSING" if "No decomposition data found" in decomp_notes else "ERROR"
    else:
        try:
            passed = config["validation"](decomp)
            result["validation_passed"] = passed
            result["status"] = "PASS" if passed else "REVIEW"
        except (KeyError, TypeError, ZeroDivisionError) as e:
            result["notes"].append(f"Validation function failed: {e}")
            result["status"] = "ERROR"

    return result


def print_article_report(result: dict[str, Any], config: dict[str, Any]) -> None:
    """Print formatted report for a single article."""
    sep = "=" * SEPARATOR_WIDTH
    print(f"\n{sep}")
    print(f"Article: {result['ad_unit']}")
    print(f"Vertical: {result['vertical']} | Fold: {result['fold']}")
    print(sep)

    # ARIMA order
    if result["arima_order"]:
        order = result["arima_order"]
        print(f"\nARIMA Order: {order['order']}")
        print(f"  Differencing (d): {order['differencing']}")
        print(f"  Has drift: {order['has_drift']}")
        print(f"  AIC: {order['aic']}")
        print(f"  Holiday effect detected: {order['has_holiday_effect']}")
        print(f"  Step changes detected: {order['has_step_changes']}")

        # Aria's question
        if order["differencing"] > 0:
            print(f"\n  [Aria] Why d={order['differencing']}? Series is non-stationary.")
    else:
        print("\nARIMA Order: Not available")

    # Decomposition
    if result["decomposition"]:
        d = result["decomposition"]
        print(f"\nDecomposition Analysis:")
        print(f"  Avg forecast: {d['avg_forecast']:,.0f}")
        print(f"  Weekend effect: {d['weekend_effect']:+.1%}")
        print(f"  Weekly amplitude: {d['weekly_amplitude']:.1%}")
        print(f"  Yearly amplitude: {d['yearly_amplitude']:.1%}")
        print(f"  Holiday impact: {d['holiday_impact_pct']:.1%} (max)")
    else:
        print("\nDecomposition: Not available")

    # Expected vs actual
    print(f"\nExpected Patterns:")
    print(f"  Weekly: {config['expected_weekly']}")
    print(f"  Annual: {config['expected_annual']}")
    print(f"  Holiday: {config['expected_holiday']}")

    # Validation
    print(f"\nValidation: {config['validation_desc']}")
    status_icon = STATUS_ICONS.get(result["status"], STATUS_ICONS["UNKNOWN"])
    print(f"  Result: {status_icon} {result['status']}")

    if result["notes"]:
        print(f"\nNotes:")
        for note in result["notes"]:
            print(f"  - {note}")


def run_spot_checks(fold_name: str) -> list[dict[str, Any]]:
    """Run spot checks on all configured articles.

    Args:
        fold_name: Validation fold to check.

    Returns:
        List of check results.
    """
    client = get_client()
    results = []

    print_summary_header(f"ARIMA_PLUS Decomposition Spot Check (Fold: {fold_name})")

    for ad_unit, config in SPOT_CHECK_ARTICLES.items():
        result = check_article(client, ad_unit, fold_name, config)
        results.append(result)
        print_article_report(result, config)

    # Summary
    print_section("Summary")

    passed = sum(1 for r in results if r["status"] == "PASS")
    review = sum(1 for r in results if r["status"] == "REVIEW")
    missing = sum(1 for r in results if r["status"] == "MISSING")
    error = sum(1 for r in results if r["status"] == "ERROR")

    print(f"  PASS: {passed}")
    print(f"  REVIEW: {review}")
    print(f"  MISSING: {missing}")
    print(f"  ERROR: {error}")

    if review > 0:
        print(f"\nArticles needing review:")
        for r in results:
            if r["status"] == "REVIEW":
                print(f"  - {r['ad_unit']}: pattern didn't match hypothesis")

    print("\n")
    return results


def main() -> None:
    """CLI entrypoint."""
    parser = argparse.ArgumentParser(
        description="Spot-check ARIMA decomposition plausibility"
    )
    parser.add_argument(
        "--fold",
        type=str,
        default="fold_1",
        choices=[f["name"] for f in settings.FOLD_CONFIGS],
        help="Fold to check (default: fold_1)",
    )

    args = parser.parse_args()
    require_project_id()

    try:
        run_spot_checks(args.fold)
    except PIPELINE_EXCEPTIONS as e:
        logger.exception("Spot check failed: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
