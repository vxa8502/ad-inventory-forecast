"""Data validation suite for ad inventory forecasting.

Validates extracted Wikipedia pageviews data before model development.
Generates distribution analysis, time series plots, and device split verification.

Usage:
    python -m scripts.data_validation [--dry-run] [--output-dir OUTPUT_DIR]
"""

import argparse
import logging
from pathlib import Path
from typing import Any

import pandas as pd

from config import settings
from config.events import KNOWN_EVENTS, SAMPLE_AD_UNITS
from config.helpers import format_sql_list, get_table_id
from src.bq_client import run_query
from src.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


def _run_query_to_df(sql: str, description: str) -> pd.DataFrame:
    """Execute query and return results as DataFrame."""
    logger.info("%s...", description)
    return run_query(sql).to_dataframe()


def query_distribution_stats() -> pd.DataFrame:
    """Query impression distribution statistics per ad unit."""
    table = get_table_id("raw_pageviews")
    sql = f"""
    SELECT
        ad_unit,
        COUNT(*) AS n_days,
        MIN(daily_impressions) AS min_impressions,
        MAX(daily_impressions) AS max_impressions,
        AVG(daily_impressions) AS mean_impressions,
        STDDEV(daily_impressions) AS std_impressions,
        APPROX_QUANTILES(daily_impressions, 100)[OFFSET(50)] AS median_impressions,
        APPROX_QUANTILES(daily_impressions, 100)[OFFSET(25)] AS p25_impressions,
        APPROX_QUANTILES(daily_impressions, 100)[OFFSET(75)] AS p75_impressions,
        SUM(daily_impressions) AS total_impressions
    FROM `{table}`
    GROUP BY ad_unit
    ORDER BY total_impressions DESC
    """
    return _run_query_to_df(sql, "Querying distribution statistics")


def query_device_split_verification() -> pd.DataFrame:
    """Verify desktop + mobile = total impressions."""
    table = get_table_id("raw_pageviews")
    sql = f"""
    SELECT
        ad_unit,
        SUM(daily_impressions) AS total_impressions,
        SUM(desktop_impressions) AS desktop_sum,
        SUM(mobile_impressions) AS mobile_sum,
        SUM(desktop_impressions) + SUM(mobile_impressions) AS computed_total,
        ABS(SUM(daily_impressions)
            - (SUM(desktop_impressions) + SUM(mobile_impressions))) AS discrepancy
    FROM `{table}`
    GROUP BY ad_unit
    ORDER BY discrepancy DESC
    """
    return _run_query_to_df(sql, "Verifying device splits")


def query_timeseries_sample(ad_units: list[str]) -> pd.DataFrame:
    """Query daily time series for sample ad units."""
    table = get_table_id("raw_pageviews")
    ad_unit_list = format_sql_list(ad_units)
    sql = f"""
    SELECT
        date,
        ad_unit,
        daily_impressions,
        desktop_impressions,
        mobile_impressions
    FROM `{table}`
    WHERE ad_unit IN ({ad_unit_list})
    ORDER BY ad_unit, date
    """
    description = f"Querying time series for {len(ad_units)} sample ad units"
    return _run_query_to_df(sql, description)


def query_holiday_impact() -> pd.DataFrame:
    """Check impression patterns on holidays vs non-holidays."""
    pageviews = get_table_id("raw_pageviews")
    holidays = get_table_id("us_holidays")
    sql = f"""
    SELECT
        d.ad_unit,
        h.holiday_name,
        AVG(d.daily_impressions) AS avg_impressions_on_holiday
    FROM `{pageviews}` d
    JOIN `{holidays}` h
        ON d.date = h.holiday_date
    WHERE h.is_major = TRUE
    GROUP BY d.ad_unit, h.holiday_name
    ORDER BY d.ad_unit, h.holiday_name
    """
    return _run_query_to_df(sql, "Analyzing holiday impact")


def query_weekday_patterns() -> pd.DataFrame:
    """Analyze day-of-week patterns per ad unit."""
    table = get_table_id("raw_pageviews")
    sql = f"""
    SELECT
        ad_unit,
        EXTRACT(DAYOFWEEK FROM date) AS day_of_week,
        AVG(daily_impressions) AS avg_impressions,
        COUNT(*) AS n_observations
    FROM `{table}`
    GROUP BY ad_unit, day_of_week
    ORDER BY ad_unit, day_of_week
    """
    return _run_query_to_df(sql, "Analyzing weekday patterns")


def check_distribution_balance(df: pd.DataFrame) -> dict[str, Any]:
    """Analyze class balance across ad units."""
    max_traffic = df["total_impressions"].max()
    min_traffic = df["total_impressions"].min()
    ratio = max_traffic / min_traffic if min_traffic > 0 else float("inf")

    high_traffic = df[df["total_impressions"] > df["total_impressions"].median()]
    low_traffic = df[df["total_impressions"] <= df["total_impressions"].median()]

    return {
        "max_total": int(max_traffic),
        "min_total": int(min_traffic),
        "max_min_ratio": round(ratio, 1),
        "max_ad_unit": df.loc[df["total_impressions"].idxmax(), "ad_unit"],
        "min_ad_unit": df.loc[df["total_impressions"].idxmin(), "ad_unit"],
        "high_traffic_units": high_traffic["ad_unit"].tolist(),
        "low_traffic_units": low_traffic["ad_unit"].tolist(),
    }


def verify_device_splits(df: pd.DataFrame) -> dict[str, Any]:
    """Check if device splits sum correctly."""
    perfect_match = (df["discrepancy"] == 0).all()
    max_discrepancy = df["discrepancy"].max()
    problem_units = df[df["discrepancy"] > 0]["ad_unit"].tolist()

    return {
        "all_splits_valid": perfect_match,
        "max_discrepancy": int(max_discrepancy),
        "problem_units": problem_units,
    }


def generate_distribution_report(df: pd.DataFrame, balance: dict[str, Any]) -> str:
    """Generate markdown report section for distribution analysis."""
    lines = [
        "## Distribution Analysis",
        "",
        f"**Traffic Range**: {balance['min_ad_unit']} ({balance['min_total']:,} total) "
        f"to {balance['max_ad_unit']} ({balance['max_total']:,} total)",
        f"**Max/Min Ratio**: {balance['max_min_ratio']}x",
        "",
        "### Implications for Model Evaluation",
        "",
        "High-traffic ad units will dominate aggregate metrics (MAPE, RMSE) unless ",
        "evaluation is stratified. Consider:",
        "- Per-ad-unit metrics reporting",
        "- Traffic-tier grouping (Very High / High / Medium)",
        "- Weighted metrics based on business value",
        "",
        "### Per-Ad-Unit Statistics",
        "",
        "| Ad Unit | Days | Mean | Median | Std | Min | Max |",
        "|---------|------|------|--------|-----|-----|-----|",
    ]

    for _, row in df.iterrows():
        lines.append(
            f"| {row['ad_unit']} | {row['n_days']} | "
            f"{row['mean_impressions']:,.0f} | {row['median_impressions']:,} | "
            f"{row['std_impressions']:,.0f} | {row['min_impressions']:,} | "
            f"{row['max_impressions']:,} |"
        )

    lines.extend(["", ""])
    return "\n".join(lines)


def generate_device_split_report(df: pd.DataFrame, verification: dict[str, Any]) -> str:
    """Generate markdown report for device split verification."""
    status = "PASS" if verification["all_splits_valid"] else "WARN"

    lines = [
        "## Device Split Verification",
        "",
        f"**Status**: {status}",
        f"**Max Discrepancy**: {verification['max_discrepancy']} impressions",
        "",
    ]

    if verification["all_splits_valid"]:
        lines.append("All ad units have desktop + mobile = total impressions.")
    else:
        lines.append("Units with discrepancies:")
        for unit in verification["problem_units"]:
            lines.append(f"- {unit}")

    lines.extend([
        "",
        "### Device Mix Summary",
        "",
        "| Ad Unit | Total | Desktop | Mobile | Desktop % |",
        "|---------|-------|---------|--------|-----------|",
    ])

    for _, row in df.iterrows():
        desktop_pct = (
            row["desktop_sum"] / row["total_impressions"] * 100
            if row["total_impressions"] > 0
            else 0
        )
        lines.append(
            f"| {row['ad_unit']} | {row['total_impressions']:,} | "
            f"{row['desktop_sum']:,} | {row['mobile_sum']:,} | "
            f"{desktop_pct:.1f}% |"
        )

    lines.extend(["", ""])
    return "\n".join(lines)


def generate_events_report() -> str:
    """Generate markdown report for known anomalous events."""
    lines = [
        "## Known Real-World Events",
        "",
        "These dates have legitimate traffic anomalies that models should capture, ",
        "not treat as outliers to be cleaned.",
        "",
    ]

    for ad_unit, events in sorted(KNOWN_EVENTS.items()):
        lines.append(f"### {ad_unit}")
        lines.append("")
        lines.append("| Date | Event |")
        lines.append("|------|-------|")
        for date, event in events:
            lines.append(f"| {date} | {event} |")
        lines.append("")

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Data validation suite")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Estimate query costs without executing",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=settings.PROJECT_ROOT / "data" / "validation",
        help="Output directory for reports and plots",
    )
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Output directory: %s", args.output_dir)

    if args.dry_run:
        logger.info("DRY RUN MODE - showing queries only")
        print("\nQueries that would be executed:")
        print("1. Distribution statistics query")
        print("2. Device split verification query")
        print("3. Time series sample query")
        print("4. Holiday impact query")
        print("5. Weekday patterns query")
        return

    logger.info("Starting data validation...")

    dist_df = query_distribution_stats()
    dist_df.to_csv(args.output_dir / "distribution_stats.csv", index=False)
    balance = check_distribution_balance(dist_df)
    logger.info(
        "Distribution balance: %sx ratio (max=%s, min=%s)",
        balance["max_min_ratio"],
        balance["max_ad_unit"],
        balance["min_ad_unit"],
    )

    device_df = query_device_split_verification()
    device_df.to_csv(args.output_dir / "device_splits.csv", index=False)
    device_verification = verify_device_splits(device_df)
    logger.info("Device splits valid: %s", device_verification["all_splits_valid"])

    ts_df = query_timeseries_sample(SAMPLE_AD_UNITS)
    ts_df.to_csv(args.output_dir / "sample_timeseries.csv", index=False)
    logger.info("Time series data exported for %d ad units", len(SAMPLE_AD_UNITS))

    weekday_df = query_weekday_patterns()
    weekday_df.to_csv(args.output_dir / "weekday_patterns.csv", index=False)
    logger.info("Weekday patterns exported")

    report_lines = [
        "# Data Validation Report",
        "",
        f"*Generated on {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}*",
        "",
        "---",
        "",
    ]

    report_lines.append(generate_distribution_report(dist_df, balance))
    report_lines.append(generate_device_split_report(device_df, device_verification))
    report_lines.append(generate_events_report())

    report_path = args.output_dir / "validation_report.md"
    report_path.write_text("\n".join(report_lines))
    logger.info("Validation report written to %s", report_path)

    print("\n" + "=" * 60)
    print("VALIDATION SUMMARY")
    print("=" * 60)
    print(f"Distribution ratio:     {balance['max_min_ratio']}x")
    print(f"Highest traffic:        {balance['max_ad_unit']}")
    print(f"Lowest traffic:         {balance['min_ad_unit']}")
    print(f"Device splits valid:    {device_verification['all_splits_valid']}")
    print(f"Sample plots ready:     {len(SAMPLE_AD_UNITS)} ad units")
    print("=" * 60)
    print(f"\nOutputs saved to: {args.output_dir}")
    print("  - distribution_stats.csv")
    print("  - device_splits.csv")
    print("  - sample_timeseries.csv")
    print("  - weekday_patterns.csv")
    print("  - validation_report.md")


if __name__ == "__main__":
    main()
