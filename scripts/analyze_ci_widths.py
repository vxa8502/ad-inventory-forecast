"""Analyze confidence interval widths for TimesFM 2.5 forecasts.

Inspects CI widths and flags implausibly narrow or wide intervals.

Usage:
    python -m scripts.analyze_ci_widths
"""

from __future__ import annotations

import logging
import sys
from typing import Any

from config import settings
from src.bq_client import get_client
from src.cli import require_project_id
from src.logging_config import setup_logging
from src.pipeline_utils import PIPELINE_EXCEPTIONS
from src.printing_utils import (
    print_dataframe_rows,
    print_section,
    print_subsection,
)

setup_logging()
logger = logging.getLogger(__name__)

__all__ = ["run_ci_analysis"]


CI_WIDTH_QUERY = """
WITH ci_stats AS (
    SELECT
        ad_unit,
        fold_name,
        COUNT(*) AS forecast_days,
        AVG(forecast_value) AS avg_forecast,
        AVG(forecast_upper - forecast_lower) AS avg_ci_width,
        MIN(forecast_upper - forecast_lower) AS min_ci_width,
        MAX(forecast_upper - forecast_lower) AS max_ci_width,
        -- Relative CI width (normalized by forecast value)
        AVG(
            SAFE_DIVIDE(
                forecast_upper - forecast_lower,
                forecast_value
            )
        ) AS avg_relative_width,
        -- CI width as % of forecast
        AVG(
            SAFE_DIVIDE(
                forecast_upper - forecast_lower,
                forecast_value
            ) * 100
        ) AS avg_ci_pct
    FROM `{project_id}.{dataset}.forecasts`
    WHERE model_name = 'timesfm_2_5'
    GROUP BY ad_unit, fold_name
)
SELECT
    ad_unit,
    fold_name,
    forecast_days,
    ROUND(avg_forecast, 0) AS avg_forecast,
    ROUND(avg_ci_width, 0) AS avg_ci_width,
    ROUND(min_ci_width, 0) AS min_ci_width,
    ROUND(max_ci_width, 0) AS max_ci_width,
    ROUND(avg_ci_pct, 1) AS avg_ci_pct,
    -- Flag anomalies (thresholds from config.settings)
    CASE
        WHEN avg_ci_pct < {ci_narrow} THEN 'NARROW'
        WHEN avg_ci_pct > {ci_wide} THEN 'WIDE'
        ELSE 'NORMAL'
    END AS ci_flag
FROM ci_stats
ORDER BY avg_ci_pct DESC
"""


OVERALL_STATS_QUERY = """
SELECT
    fold_name,
    COUNT(DISTINCT ad_unit) AS num_articles,
    ROUND(AVG(forecast_upper - forecast_lower), 0) AS overall_avg_ci_width,
    ROUND(
        AVG(
            SAFE_DIVIDE(forecast_upper - forecast_lower, forecast_value) * 100
        ), 1
    ) AS overall_avg_ci_pct,
    ROUND(
        STDDEV(
            SAFE_DIVIDE(forecast_upper - forecast_lower, forecast_value) * 100
        ), 1
    ) AS ci_pct_stddev
FROM `{project_id}.{dataset}.forecasts`
WHERE model_name = 'timesfm_2_5'
GROUP BY fold_name
ORDER BY fold_name
"""


def run_ci_analysis() -> dict[str, Any]:
    """Run confidence interval width analysis on TimesFM forecasts.

    Returns:
        Dictionary with analysis results.
    """
    client = get_client()

    results = {
        "overall": [],
        "by_article": [],
        "narrow_flags": [],
        "wide_flags": [],
    }

    # Overall stats
    print_section("TimesFM 2.5 Confidence Interval Analysis")

    overall_query = OVERALL_STATS_QUERY.format(
        project_id=settings.PROJECT_ID,
        dataset=settings.DATASET,
    )
    overall_df = client.query(overall_query).to_dataframe()

    print_subsection("Overall CI Statistics by Fold")
    print_dataframe_rows(
        overall_df,
        columns=["fold_name", "num_articles", "overall_avg_ci_width", "overall_avg_ci_pct", "ci_pct_stddev"],
        headers=["Fold", "Articles", "Avg Width", "Avg CI %", "Std Dev"],
        widths=[10, 10, 12, 10, 10],
        formatters={
            "overall_avg_ci_width": "{:>10,.0f}",
            "overall_avg_ci_pct": "{:>8.1f}%",
            "ci_pct_stddev": "{:>8.1f}%",
        },
    )
    results["overall"] = overall_df.to_dict(orient="records")

    # Per-article stats
    article_query = CI_WIDTH_QUERY.format(
        project_id=settings.PROJECT_ID,
        dataset=settings.DATASET,
        ci_narrow=settings.CI_NARROW_THRESHOLD_PCT,
        ci_wide=settings.CI_WIDE_THRESHOLD_PCT,
    )
    article_df = client.query(article_query).to_dataframe()

    # Separate by flag
    narrow = article_df[article_df["ci_flag"] == "NARROW"]
    wide = article_df[article_df["ci_flag"] == "WIDE"]
    normal = article_df[article_df["ci_flag"] == "NORMAL"]

    print_section("Flagged Articles")

    # Wide CIs
    ci_wide = settings.CI_WIDE_THRESHOLD_PCT
    ci_narrow = settings.CI_NARROW_THRESHOLD_PCT
    display_limit = settings.MAX_DISPLAY_ITEMS
    if len(wide) > 0:
        print_subsection(f"[WIDE] CI > {ci_wide}% of forecast ({len(wide)} article-folds)")
        print_dataframe_rows(
            wide,
            columns=["ad_unit", "fold_name", "avg_ci_pct", "avg_ci_width"],
            headers=["Article", "Fold", "Avg CI %", "Avg Width"],
            widths=[30, 8, 10, 12],
            formatters={"avg_ci_pct": "{:>8.1f}%", "avg_ci_width": "{:>10,.0f}"},
            max_rows=display_limit,
            truncate_col="ad_unit",
        )
        results["wide_flags"] = wide.head(display_limit).to_dict(orient="records")
    else:
        print(f"\n[WIDE] No articles with CI > {ci_wide}% of forecast")

    # Narrow CIs
    if len(narrow) > 0:
        print_subsection(f"[NARROW] CI < {ci_narrow}% of forecast ({len(narrow)} article-folds)")
        print_dataframe_rows(
            narrow,
            columns=["ad_unit", "fold_name", "avg_ci_pct", "avg_forecast"],
            headers=["Article", "Fold", "Avg CI %", "Avg Forecast"],
            widths=[30, 8, 10, 15],
            formatters={"avg_ci_pct": "{:>8.1f}%", "avg_forecast": "{:>13,.0f}"},
            max_rows=display_limit,
            truncate_col="ad_unit",
        )
        results["narrow_flags"] = narrow.head(display_limit).to_dict(orient="records")
    else:
        print(f"\n[NARROW] No articles with CI < {ci_narrow}% of forecast")

    # Summary
    print_section("Summary")
    total_article_folds = len(article_df)
    print(f"Total article-folds analyzed: {total_article_folds}")
    print(f"  NORMAL ({ci_narrow}-{ci_wide}% CI): {len(normal)} ({100*len(normal)/total_article_folds:.0f}%)")
    print(f"  NARROW (<{ci_narrow}% CI):    {len(narrow)} ({100*len(narrow)/total_article_folds:.0f}%)")
    print(f"  WIDE (>{ci_wide}% CI):     {len(wide)} ({100*len(wide)/total_article_folds:.0f}%)")

    # Interpretation
    print_section("Interpretation")

    if len(wide) > 0:
        print("\nWIDE intervals indicate high uncertainty. Common causes:")
        print("  - High traffic volatility (event-driven content)")
        print("  - Trend breaks or level shifts in training data")
        print("  - Insufficient training history for pattern detection")

    if len(narrow) > 0:
        print("\nNARROW intervals may indicate overconfidence. Check:")
        print("  - Whether actuals fall outside predicted bounds")
        print("  - Coverage metric in model_metrics table")
        print("  - High-traffic articles with stable patterns")

    if len(wide) == 0 and len(narrow) == 0:
        print(f"\nAll intervals within normal range ({ci_narrow}-{ci_wide}% of forecast).")
        print("TimesFM confidence calibration appears reasonable.")

    print("\n")

    results["by_article"] = article_df.to_dict(orient="records")
    return results


def main() -> None:
    """CLI entrypoint."""
    require_project_id()

    try:
        run_ci_analysis()
    except PIPELINE_EXCEPTIONS as e:
        logger.exception("CI analysis failed: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
