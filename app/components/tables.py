"""DataFrame formatters and table components."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from config import settings
from config.events import get_forecastability_guidance
from app.constants import (
    format_model_name,
    format_metric_name,
    format_ad_unit_name,
    CHART_HEIGHT_COMPACT,
    COLUMNS_BUSINESS_IMPACT,
    COLUMNS_ANOMALIES,
    FORMAT_BUSINESS_IMPACT,
    FORMAT_ANOMALIES,
    HIGHLIGHT_BEST_COLOR,
    MAXIMIZE_METRICS,
    METRIC_INTERNAL_NAMES,
    METRIC_MAPE,
)
from app.utils.helpers import apply_table_formatting, reorder_pivot, require_dataframe, to_iso_date_str


def metrics_table(df: pd.DataFrame, highlight_best: bool = True) -> None:
    """Display formatted metrics comparison table.

    Args:
        df: DataFrame with model_name, metric_name, metric_value.
        highlight_best: Whether to highlight best values.
    """
    if not require_dataframe(df, "No metrics data available."):
        return

    # Copy on entry to avoid SettingWithCopyWarning from mutation.
    # The incoming df may be a view (e.g., from df[df["x"] == y]).
    df_clean = df.copy().drop_duplicates(subset=["model_name", "metric_name"])

    # Pivot to model x metric format
    pivot = df_clean.pivot(
        index="model_name",
        columns="metric_name",
        values="metric_value",
    )

    # Reorder rows and columns by settings order
    pivot = reorder_pivot(pivot, settings.MODEL_NAMES, settings.METRIC_NAMES)

    # Format display
    pivot.index = pivot.index.map(format_model_name)
    pivot.columns = pivot.columns.map(format_metric_name)

    # Style the dataframe
    styled = pivot.style.format("{:.2f}")

    if highlight_best:
        for col in pivot.columns:
            # Use reverse lookup to get internal metric name from display name
            metric_internal = METRIC_INTERNAL_NAMES.get(col, col.lower())
            if metric_internal in MAXIMIZE_METRICS:
                styled = styled.highlight_max(subset=[col], color=HIGHLIGHT_BEST_COLOR, axis=0)
            else:
                styled = styled.highlight_min(subset=[col], color=HIGHLIGHT_BEST_COLOR, axis=0)

    st.dataframe(styled, width="stretch")


def business_impact_table(df: pd.DataFrame) -> None:
    """Display business impact table sorted by revenue at risk.

    Args:
        df: DataFrame with model_name, ad_unit, avg_daily_impressions,
            mape, cpm, daily_revenue_at_risk, annual_revenue_at_risk.
    """
    if not require_dataframe(df, "No business impact data available."):
        return

    sorted_df = df.sort_values("annual_revenue_at_risk", ascending=False)

    styled = apply_table_formatting(
        sorted_df,
        column_formatters={"model_name": format_model_name, "ad_unit": format_ad_unit_name},
        column_renames=COLUMNS_BUSINESS_IMPACT,
        style_formats=FORMAT_BUSINESS_IMPACT,
    )

    st.dataframe(styled, width="stretch", height=CHART_HEIGHT_COMPACT)


def anomaly_events_table(
    anomalies_df: pd.DataFrame,
    known_events: dict[str, list[tuple[str, str, str]]],
) -> None:
    """Display anomalies cross-referenced with known events.

    Args:
        anomalies_df: DataFrame with date, ad_unit, daily_impressions,
            anomaly_probability.
        known_events: Dict mapping ad_unit to list of (date, event_name, category).
    """
    if not require_dataframe(anomalies_df, "No anomalies detected above threshold."):
        return

    # Build events DataFrame for merge (pd.DataFrame handles empty list correctly)
    events_df = pd.DataFrame(
        [
            (unit, date, name, category)
            for unit, unit_events in known_events.items()
            for date, name, category in unit_events
        ],
        columns=["ad_unit", "date_str", "event", "category"],
    )

    # Prepare display DataFrame.
    # Copy before adding computed column to avoid SettingWithCopyWarning.
    display_df = anomalies_df[
        ["date", "ad_unit", "daily_impressions", "anomaly_probability"]
    ].copy()
    display_df["date_str"] = display_df["date"].apply(to_iso_date_str)

    # Merge events (left join preserves all anomalies; handles empty events_df correctly)
    display_df = display_df.merge(events_df, on=["ad_unit", "date_str"], how="left")
    display_df[["event", "category"]] = display_df[["event", "category"]].fillna("")

    # Add forecastability guidance based on category
    display_df["forecastability"] = display_df["category"].apply(get_forecastability_guidance)

    display_df = display_df.drop(columns=["date_str"])

    styled = apply_table_formatting(
        display_df,
        column_formatters={"ad_unit": format_ad_unit_name},
        column_renames=COLUMNS_ANOMALIES,
        style_formats=FORMAT_ANOMALIES,
    )

    st.dataframe(styled, width="stretch", height=CHART_HEIGHT_COMPACT)


def summary_metrics_card(
    metrics_df: pd.DataFrame,
    metric_name: str = METRIC_MAPE,
) -> None:
    """Display summary metric cards for each model.

    Args:
        metrics_df: DataFrame with model_name, metric_name, metric_value.
        metric_name: Which metric to display.
    """
    # Early exit for empty input (no user message needed - parent handles context)
    if metrics_df.empty:
        return

    # Filter to requested metric and validate
    metric_data = metrics_df[metrics_df["metric_name"] == metric_name]
    message = f"No {metric_name.upper()} metrics available."

    if not require_dataframe(metric_data, message):
        return

    # Vectorized filtering: keep only models in settings order that have data
    available = metric_data[metric_data["model_name"].isin(settings.MODEL_NAMES)]

    # Reorder by settings.MODEL_NAMES for consistent display
    available = available.set_index("model_name").reindex(settings.MODEL_NAMES).dropna()

    if not require_dataframe(available, message):
        return

    cols = st.columns(len(available))
    for col, (model, row) in zip(cols, available.iterrows()):
        with col:
            st.metric(
                label=format_model_name(model),
                value=f"{row['metric_value']:.1f}%",
            )


