"""Pre-model sanity checks for time series data quality.

Validates data quality before model training:
1. Distribution inspection - flag articles with >20% missing days or flat zeros
2. Date continuity verification - ensure contiguous daily records
3. Step change detection - identify sudden permanent traffic shifts
4. Holiday table coverage - verify holidays span training + forecast horizon

Usage:
    python -m scripts.pre_model_checks
"""

from __future__ import annotations

import argparse
import logging
from collections import defaultdict
from datetime import timedelta
from typing import Any, TypedDict

from config import settings
from config.helpers import get_date_range, get_table_id
from src.bq_client import query_to_list, run_query
from src.cli import add_verbose_arg, configure_logging_from_args, require_project_id
from src.logging_config import setup_logging
from src.printing_utils import SEPARATOR_WIDTH, print_section

setup_logging()
logger = logging.getLogger(__name__)


def _print_flag_summary(flag_type: str, flagged: list[dict[str, Any]]) -> None:
    """Print summary of flagged articles.

    Args:
        flag_type: Description of what was flagged (e.g., "missing days").
        flagged: List of flagged article dictionaries with 'ad_unit' key.
    """
    if flagged:
        print(f"\nFlagged for {flag_type}: {[a['ad_unit'] for a in flagged]}")
    else:
        print(f"\nNo articles flagged for {flag_type}.")


__all__ = [
    "check_distribution",
    "check_date_continuity",
    "check_step_changes",
    "check_holiday_coverage",
    "run_all_checks",
    "DistributionResult",
    "ContinuityResult",
    "StepChangeResult",
    "HolidayCoverageResult",
]


class DistributionResult(TypedDict):
    """Result from distribution check."""

    stats: list[dict[str, Any]]
    flagged_missing: list[dict[str, Any]]
    flagged_zeros: list[dict[str, Any]]
    threshold_pct: float


class ContinuityResult(TypedDict):
    """Result from date continuity check."""

    gap_count: int
    gaps: list[dict[str, Any]]
    status: str


class StepChangeResult(TypedDict):
    """Result from step change detection."""

    articles_with_shifts: list[str]
    shift_count: int
    details: dict[str, list[dict[str, Any]]]
    window_days: int
    threshold_ratio: float


class HolidayCoverageResult(TypedDict):
    """Result from holiday coverage check."""

    holiday_count: int
    holiday_min: str
    holiday_max: str
    train_start: str
    train_end: str
    forecast_end: str
    covers_training: bool
    covers_forecast: bool
    status: str


def check_distribution(
    threshold_pct: float = settings.DISTRIBUTION_MISSING_PCT_THRESHOLD,
) -> DistributionResult:
    """Inspect training data distribution per article.

    Flags articles with:
    - >threshold_pct missing days
    - >threshold_pct flat zeros (consecutive zero pageview days)

    Args:
        threshold_pct: Percentage threshold for flagging issues.

    Returns:
        Dict with distribution stats and flagged articles.
    """
    table_id = get_table_id("daily_impressions")
    _, _, expected_days = get_date_range()

    sql = f"""
    WITH article_stats AS (
        SELECT
            ad_unit,
            COUNT(*) AS actual_days,
            {expected_days} AS expected_days,
            COUNTIF(daily_impressions = 0) AS zero_days,
            COUNTIF(daily_impressions < 100) AS low_traffic_days,
            MIN(daily_impressions) AS min_impressions,
            MAX(daily_impressions) AS max_impressions,
            AVG(daily_impressions) AS avg_impressions,
            STDDEV(daily_impressions) AS stddev_impressions
        FROM `{table_id}`
        GROUP BY ad_unit
    )
    SELECT
        ad_unit,
        actual_days,
        expected_days,
        ROUND(100.0 * (expected_days - actual_days) / expected_days, 2) AS missing_pct,
        zero_days,
        ROUND(100.0 * zero_days / actual_days, 2) AS zero_pct,
        low_traffic_days,
        min_impressions,
        max_impressions,
        ROUND(avg_impressions, 0) AS avg_impressions,
        ROUND(stddev_impressions, 0) AS stddev_impressions,
        ROUND(stddev_impressions / NULLIF(avg_impressions, 0), 2) AS cv
    FROM article_stats
    ORDER BY missing_pct DESC, zero_pct DESC
    """

    stats = query_to_list(sql)

    flagged_missing = [s for s in stats if s["missing_pct"] > threshold_pct]
    flagged_zeros = [s for s in stats if s["zero_pct"] > threshold_pct]

    return {
        "stats": stats,
        "flagged_missing": flagged_missing,
        "flagged_zeros": flagged_zeros,
        "threshold_pct": threshold_pct,
    }


def check_date_continuity() -> ContinuityResult:
    """Verify no date gaps within each time series.

    Returns:
        Dict with gap count and details of any gaps found.
    """
    table_id = get_table_id("daily_impressions")

    sql = f"""
    WITH date_gaps AS (
        SELECT
            ad_unit,
            date,
            LAG(date) OVER (PARTITION BY ad_unit ORDER BY date) AS prev_date,
            DATE_DIFF(date, LAG(date) OVER (PARTITION BY ad_unit ORDER BY date), DAY) AS gap_days
        FROM `{table_id}`
    )
    SELECT ad_unit, date, prev_date, gap_days
    FROM date_gaps
    WHERE gap_days > 1
    ORDER BY ad_unit, date
    """

    gaps = query_to_list(sql)

    return {
        "gap_count": len(gaps),
        "gaps": gaps,
        "status": "PASS" if len(gaps) == 0 else "FAIL",
    }


def check_step_changes(
    window_days: int = settings.STEP_CHANGE_WINDOW_DAYS,
    threshold_ratio: float = settings.STEP_CHANGE_RATIO_THRESHOLD,
) -> StepChangeResult:
    """Detect sudden permanent traffic shifts in each series.

    Compares rolling averages before/after each date to detect level shifts.

    Args:
        window_days: Days to use for rolling average calculation.
        threshold_ratio: Minimum ratio between periods to flag as step change.

    Returns:
        Dict with detected step changes per article.
    """
    table_id = get_table_id("daily_impressions")

    sql = f"""
    WITH rolling_stats AS (
        SELECT
            ad_unit,
            date,
            daily_impressions,
            AVG(daily_impressions) OVER (
                PARTITION BY ad_unit
                ORDER BY date
                ROWS BETWEEN {window_days} PRECEDING AND 1 PRECEDING
            ) AS rolling_avg_before,
            AVG(daily_impressions) OVER (
                PARTITION BY ad_unit
                ORDER BY date
                ROWS BETWEEN 1 FOLLOWING AND {window_days} FOLLOWING
            ) AS rolling_avg_after
        FROM `{table_id}`
    ),
    step_changes AS (
        SELECT
            ad_unit,
            date,
            daily_impressions,
            ROUND(rolling_avg_before, 0) AS avg_before,
            ROUND(rolling_avg_after, 0) AS avg_after,
            ROUND(rolling_avg_after / NULLIF(rolling_avg_before, 0), 2) AS change_ratio,
            ROUND(rolling_avg_before / NULLIF(rolling_avg_after, 0), 2) AS inverse_ratio
        FROM rolling_stats
        WHERE rolling_avg_before IS NOT NULL
          AND rolling_avg_after IS NOT NULL
          AND rolling_avg_before > 0
          AND rolling_avg_after > 0
    )
    SELECT *
    FROM step_changes
    WHERE change_ratio >= {threshold_ratio} OR inverse_ratio >= {threshold_ratio}
    ORDER BY ad_unit, date
    """

    changes = query_to_list(sql)

    articles_with_shifts = defaultdict(list)
    for change in changes:
        articles_with_shifts[change["ad_unit"]].append(change)

    return {
        "articles_with_shifts": list(articles_with_shifts.keys()),
        "shift_count": len(articles_with_shifts),
        "details": articles_with_shifts,
        "window_days": window_days,
        "threshold_ratio": threshold_ratio,
    }


def check_holiday_coverage() -> HolidayCoverageResult:
    """Verify holiday table spans training window + forecast horizon.

    Returns:
        Dict with holiday coverage analysis.
    """
    table_id = get_table_id("us_holidays")

    sql = f"SELECT MIN(holiday_date) AS min_date, MAX(holiday_date) AS max_date, COUNT(*) AS total FROM `{table_id}`"

    row = next(iter(run_query(sql)))

    train_start, train_end, _ = get_date_range()
    forecast_end = train_end + timedelta(days=settings.FORECAST_HORIZON)

    holiday_min = row.min_date
    holiday_max = row.max_date

    covers_training = holiday_min <= train_start and holiday_max >= train_end
    covers_forecast = holiday_max >= forecast_end

    return {
        "holiday_count": row.total,
        "holiday_min": str(holiday_min),
        "holiday_max": str(holiday_max),
        "train_start": str(train_start),
        "train_end": str(train_end),
        "forecast_end": str(forecast_end),
        "covers_training": covers_training,
        "covers_forecast": covers_forecast,
        "status": "PASS" if covers_training and covers_forecast else "FAIL",
    }


def print_distribution_report(result: dict) -> None:
    """Print formatted distribution check report."""
    print_section("CHECK 1: Training Data Distribution")

    stats = result["stats"]
    threshold = result["threshold_pct"]

    print(f"\nThreshold: >{threshold}% missing days or flat zeros flags an article\n")
    print(f"{'Article':<40} {'Missing%':>8} {'Zero%':>8} {'CV':>6} {'Avg':>10}")
    print("-" * SEPARATOR_WIDTH)

    for s in stats:
        flag = " [FLAG]" if s["missing_pct"] > threshold or s["zero_pct"] > threshold else ""
        print(
            f"{s['ad_unit'][:38]:<40} {s['missing_pct']:>7.1f}% {s['zero_pct']:>7.1f}% "
            f"{s['cv']:>6.2f} {s['avg_impressions']:>10,.0f}{flag}"
        )

    print("-" * SEPARATOR_WIDTH)

    _print_flag_summary("missing days", result["flagged_missing"])
    _print_flag_summary("flat zeros", result["flagged_zeros"])


def print_continuity_report(result: dict) -> None:
    """Print formatted date continuity report."""
    print_section("CHECK 2: Date Continuity")

    print(f"\nStatus: {result['status']}")
    print(f"Gaps found: {result['gap_count']}")

    if result["gaps"]:
        print("\nGap details:")
        display_limit = settings.MAX_DISPLAY_ITEMS
        for gap in result["gaps"][:display_limit]:
            print(f"  {gap['ad_unit']}: {gap['prev_date']} -> {gap['date']} ({gap['gap_days']} days)")
        if len(result["gaps"]) > display_limit:
            print(f"  ... and {len(result['gaps']) - display_limit} more")


def print_step_change_report(result: dict) -> None:
    """Print formatted step change detection report."""
    print_section("CHECK 3: Step Changes (Sudden Traffic Shifts)")

    print(f"\nWindow: {result['window_days']} days rolling average")
    print(f"Threshold: {result['threshold_ratio']}x change ratio")
    print(f"Articles with detected shifts: {result['shift_count']}")

    if result["articles_with_shifts"]:
        print("\nArticles with step changes:")
        for ad_unit in result["articles_with_shifts"]:
            shifts = result["details"][ad_unit]
            first_shift = shifts[0]
            direction = "UP" if first_shift["change_ratio"] >= result["threshold_ratio"] else "DOWN"
            print(f"  {ad_unit}: {len(shifts)} shift(s), first on {first_shift['date']} ({direction})")
    else:
        print("\nNo significant step changes detected.")


def print_holiday_report(result: dict) -> None:
    """Print formatted holiday coverage report."""
    print_section("CHECK 4: Holiday Table Coverage")

    print(f"\nHoliday count: {result['holiday_count']}")
    print(f"Holiday range: {result['holiday_min']} to {result['holiday_max']}")
    print(f"Training period: {result['train_start']} to {result['train_end']}")
    print(f"Forecast horizon end: {result['forecast_end']}")
    print(f"\nCovers training period: {'Yes' if result['covers_training'] else 'No'}")
    print(f"Covers forecast horizon: {'Yes' if result['covers_forecast'] else 'No'}")
    print(f"\nStatus: {result['status']}")


def _evaluate_check_status(results: dict, key: str) -> bool:
    """Evaluate whether a check passed based on its results.

    Args:
        results: Full results dictionary.
        key: Which check to evaluate.

    Returns:
        True if check passed, False if needs review.
    """
    evaluators = {
        "distribution": lambda r: (
            len(r["flagged_missing"]) == 0 and len(r["flagged_zeros"]) == 0
        ),
        "continuity": lambda r: r["status"] == "PASS",
        "step_changes": lambda r: r["shift_count"] == 0,
        "holidays": lambda r: r["status"] == "PASS",
    }
    return evaluators[key](results[key])


# Check registry: (key, display_name, check_func, report_func)
_CHECK_REGISTRY = [
    ("distribution", "Distribution", check_distribution, print_distribution_report),
    ("continuity", "Date continuity", check_date_continuity, print_continuity_report),
    ("step_changes", "Step changes", check_step_changes, print_step_change_report),
    ("holidays", "Holiday coverage", check_holiday_coverage, print_holiday_report),
]


def run_all_checks() -> dict:
    """Run all pre-model sanity checks and return results."""
    print_section("PRE-MODEL SANITY CHECKS")

    results = {}

    for key, display_name, check_func, report_func in _CHECK_REGISTRY:
        print(f"\nRunning {display_name.lower()} check...")
        results[key] = check_func()
        report_func(results[key])

    print_section("SUMMARY")

    for key, display_name, _, _ in _CHECK_REGISTRY:
        passed = _evaluate_check_status(results, key)
        status = "PASS" if passed else "REVIEW"
        print(f"  [{status}] {display_name}")

    print()
    return results


def main() -> None:
    """CLI entrypoint."""
    parser = argparse.ArgumentParser(description="Run pre-model sanity checks")
    add_verbose_arg(parser)

    args = parser.parse_args()
    configure_logging_from_args(args)
    require_project_id()

    run_all_checks()


if __name__ == "__main__":
    main()
