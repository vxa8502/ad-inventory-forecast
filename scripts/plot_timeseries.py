"""Time series visualization for data validation.

Generates diagnostic plots to verify seasonality, holiday spikes, and trends
before model development.

Usage:
    python -m scripts.plot_timeseries [--input-dir INPUT_DIR] [--output-dir OUTPUT_DIR]
"""

from __future__ import annotations

import argparse
import logging
from functools import lru_cache
from pathlib import Path

import pandas as pd

try:
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt
except ImportError:
    print("matplotlib required: pip install matplotlib")
    raise

from config import settings
from config.events import KNOWN_EVENTS
from src.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

__all__ = [
    "get_us_holidays",
    "plot_single_timeseries",
    "plot_weekday_heatmap",
    "plot_distribution_comparison",
]

# Import centralized colors from config
COLORS = settings.PLOT_COLORS


def _save_figure(fig: plt.Figure, path: Path) -> None:
    """Save figure with consistent settings and cleanup."""
    plt.tight_layout()
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("Saved plot: %s", path)


@lru_cache(maxsize=1)
def get_us_holidays() -> tuple[str, ...]:
    """Load holiday dates from reference CSV (cached via lru_cache).

    Returns:
        Tuple of holiday date strings in YYYY-MM-DD format.
    """
    csv_path = settings.PROJECT_ROOT / "data" / "reference" / "us_holidays.csv"
    if csv_path.exists():
        df = pd.read_csv(csv_path)
        return tuple(df["holiday_date"].tolist())
    return ()


def plot_single_timeseries(
    df: pd.DataFrame,
    ad_unit: str,
    output_path: Path,
    show_events: bool = True,
    show_holidays: bool = True,
) -> None:
    """Generate time series plot for a single ad unit."""
    unit_df = df[df["ad_unit"] == ad_unit].copy()
    unit_df["date"] = pd.to_datetime(unit_df["date"])
    unit_df = unit_df.sort_values("date")

    fig, axes = plt.subplots(3, 1, figsize=settings.PLOT_FIGURE_SIZE, sharex=True)
    fig.suptitle(f"Time Series Diagnostics: {ad_unit}", fontsize=14, fontweight="bold")

    ax1 = axes[0]
    ax1.plot(
        unit_df["date"], unit_df["daily_impressions"],
        linewidth=0.8, color=COLORS["primary"]
    )
    ax1.set_ylabel("Daily Impressions")
    ax1.set_title("Raw Time Series with Events")
    ax1.grid(True, alpha=0.3)

    if show_holidays:
        for holiday in get_us_holidays():
            holiday_date = pd.to_datetime(holiday)
            date_min, date_max = unit_df["date"].min(), unit_df["date"].max()
            if date_min <= holiday_date <= date_max:
                ax1.axvline(
                    holiday_date, color="green", alpha=0.3, linestyle="--", linewidth=1
                )

    if show_events and ad_unit in KNOWN_EVENTS:
        for event_date, event_name in KNOWN_EVENTS[ad_unit]:
            event_dt = pd.to_datetime(event_date)
            if date_min <= event_dt <= date_max:
                ax1.axvline(
                    event_dt, color="red", alpha=0.7, linestyle="-", linewidth=1.5
                )
                ypos = unit_df["daily_impressions"].max() * settings.PLOT_ANNOTATION_HEIGHT_PCT
                ax1.annotate(
                    event_name,
                    xy=(event_dt, ypos),
                    fontsize=8,
                    ha="left",
                    rotation=45,
                    color="red",
                )

    ax2 = axes[1]
    impressions = unit_df["daily_impressions"]
    unit_df["rolling_7d"] = impressions.rolling(7, center=True).mean()
    unit_df["rolling_30d"] = impressions.rolling(30, center=True).mean()
    ax2.plot(
        unit_df["date"], unit_df["rolling_7d"],
        linewidth=1.2, color=COLORS["secondary"], label="7-day MA"
    )
    ax2.plot(
        unit_df["date"], unit_df["rolling_30d"],
        linewidth=1.5, color=COLORS["dark"], label="30-day MA"
    )
    ax2.set_ylabel("Impressions (MA)")
    ax2.set_title("Trend Detection (Moving Averages)")
    ax2.legend(loc="upper right")
    ax2.grid(True, alpha=0.3)

    ax3 = axes[2]
    ax3.fill_between(
        unit_df["date"],
        unit_df["desktop_impressions"],
        label="Desktop",
        alpha=0.7,
        color=COLORS["desktop"],
    )
    ax3.fill_between(
        unit_df["date"],
        unit_df["desktop_impressions"],
        unit_df["desktop_impressions"] + unit_df["mobile_impressions"],
        label="Mobile",
        alpha=0.7,
        color=COLORS["mobile"],
    )
    ax3.set_ylabel("Impressions")
    ax3.set_xlabel("Date")
    ax3.set_title("Device Split Over Time")
    ax3.legend(loc="upper right")
    ax3.grid(True, alpha=0.3)

    ax3.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax3.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    plt.setp(ax3.xaxis.get_majorticklabels(), rotation=45, ha="right")

    _save_figure(fig, output_path)


def plot_weekday_heatmap(df: pd.DataFrame, output_path: Path) -> None:
    """Generate weekday pattern heatmap across all ad units."""
    pivot = df.pivot(index="ad_unit", columns="day_of_week", values="avg_impressions")

    pivot_normalized = pivot.div(pivot.max(axis=1), axis=0)

    fig, ax = plt.subplots(figsize=(10, 12))

    im = ax.imshow(pivot_normalized.values, aspect="auto", cmap="YlOrRd")

    ax.set_xticks(range(7))
    ax.set_xticklabels(["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"])
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels(pivot.index, fontsize=8)

    ax.set_title(
        "Weekday Traffic Patterns (Normalized by Ad Unit)",
        fontsize=12, fontweight="bold"
    )
    ax.set_xlabel("Day of Week")
    ax.set_ylabel("Ad Unit")

    cbar = plt.colorbar(im, ax=ax, shrink=0.6)
    cbar.set_label("Relative Traffic (0=min, 1=max)")

    _save_figure(fig, output_path)


def plot_distribution_comparison(df: pd.DataFrame, output_path: Path) -> None:
    """Generate distribution comparison across ad units."""
    df_sorted = df.sort_values("total_impressions", ascending=True)

    fig, ax = plt.subplots(figsize=(10, 12))

    median = df["total_impressions"].median()
    colors = [COLORS["above_median"] if x > median else COLORS["below_median"]
              for x in df_sorted["total_impressions"]]

    ax.barh(df_sorted["ad_unit"], df_sorted["total_impressions"] / 1e6, color=colors)

    ax.set_xlabel("Total Impressions (Millions)")
    ax.set_ylabel("Ad Unit")
    ax.set_title(
        "Traffic Distribution Across Ad Units (2023-2024)",
        fontsize=12, fontweight="bold"
    )

    above = plt.Rectangle((0, 0), 1, 1, color=COLORS["above_median"])
    below = plt.Rectangle((0, 0), 1, 1, color=COLORS["below_median"])
    ax.legend(
        handles=[above, below],
        labels=["Above Median", "Below Median"],
        loc="lower right",
    )

    _save_figure(fig, output_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Time Series Visualization")
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=settings.PROJECT_ROOT / "data" / "validation",
        help="Input directory with validation CSVs",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=settings.PROJECT_ROOT / "data" / "validation" / "plots",
        help="Output directory for plots",
    )
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)

    ts_path = args.input_dir / "sample_timeseries.csv"
    if not ts_path.exists():
        logger.error("Time series CSV not found: %s", ts_path)
        logger.error("Run 'python scripts/data_validation.py' first")
        return

    ts_df = pd.read_csv(ts_path)
    logger.info("Loaded time series data: %d rows", len(ts_df))

    ad_units = ts_df["ad_unit"].unique()
    for ad_unit in ad_units:
        output_path = args.output_dir / f"ts_{ad_unit}.png"
        plot_single_timeseries(ts_df, ad_unit, output_path)

    weekday_path = args.input_dir / "weekday_patterns.csv"
    if weekday_path.exists():
        weekday_df = pd.read_csv(weekday_path)
        plot_weekday_heatmap(weekday_df, args.output_dir / "weekday_heatmap.png")

    dist_path = args.input_dir / "distribution_stats.csv"
    if dist_path.exists():
        dist_df = pd.read_csv(dist_path)
        dist_plot = args.output_dir / "distribution_comparison.png"
        plot_distribution_comparison(dist_df, dist_plot)

    print(f"\nPlots saved to: {args.output_dir}")
    print(f"  - {len(ad_units)} time series plots (ts_*.png)")
    print("  - weekday_heatmap.png")
    print("  - distribution_comparison.png")


if __name__ == "__main__":
    main()
