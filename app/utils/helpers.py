"""Shared utility functions for the dashboard."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Sequence

import pandas as pd
import streamlit as st
from pandas.io.formats.style import Styler

from config import settings
from app.constants import (
    CONTENT_TYPE_ALL,
    CONTENT_TYPE_STABLE,
    CONTENT_TYPE_EVENT_DRIVEN,
    DEFAULT_DETAIL_FOLD,
    FOLD_CROSS_AVG,
    FOLD_LABELS,
    METRIC_MAPE,
    format_ad_unit_name,
    format_model_name,
)

if TYPE_CHECKING:
    import plotly.graph_objects as go


# Date and lookup utilities


def to_iso_date_str(date_value: Any) -> str:
    """Convert datetime/Timestamp/string to ISO date string YYYY-MM-DD.

    Handles datetime, pandas Timestamp, and string inputs robustly.
    Centralizes the repeated `str(date)[:10]` pattern.

    Args:
        date_value: Any date-like value (datetime, Timestamp, or string).

    Returns:
        ISO format date string (YYYY-MM-DD).

    Example:
        >>> to_iso_date_str(pd.Timestamp("2024-01-15 12:30:00"))
        '2024-01-15'
        >>> to_iso_date_str("2024-01-15")
        '2024-01-15'
    """
    if hasattr(date_value, "strftime"):
        return date_value.strftime("%Y-%m-%d")
    return str(date_value)[:10]


def build_lookup_dict(
    df: pd.DataFrame,
    key_col: str,
    value_col: str,
    key_transform: Callable[[Any], Any] | None = None,
) -> dict:
    """Build lookup dictionary from DataFrame columns.

    Centralizes the repeated pattern of converting DataFrames to dicts
    for O(1) lookups. Handles empty DataFrames gracefully.

    Args:
        df: DataFrame to extract lookup from.
        key_col: Column name to use as dictionary keys.
        value_col: Column name to use as dictionary values.
        key_transform: Optional function to transform keys (e.g., to_iso_date_str).

    Returns:
        Dictionary mapping key_col values to value_col values.
        Returns empty dict if DataFrame is empty or None.

    Example:
        >>> df = pd.DataFrame({"ad_unit": ["A", "B"], "cv": [0.3, 0.7]})
        >>> build_lookup_dict(df, "ad_unit", "cv")
        {'A': 0.3, 'B': 0.7}

        >>> # With key transform for dates:
        >>> build_lookup_dict(df, "date", "effect", key_transform=to_iso_date_str)
    """
    if df is None or df.empty:
        return {}
    keys = df[key_col].apply(key_transform) if key_transform else df[key_col]
    return dict(zip(keys, df[value_col]))


# Aggregation functions for decomposition statistics
def series_range(s: pd.Series) -> float:
    """Calculate range (max - min) of a Series.

    Args:
        s: Pandas Series to aggregate.

    Returns:
        Difference between maximum and minimum values.
    """
    return float(s.max() - s.min())


def series_max_abs(s: pd.Series) -> float:
    """Calculate maximum absolute value of a Series.

    Args:
        s: Pandas Series to aggregate.

    Returns:
        Maximum of absolute values.
    """
    return float(s.abs().max())


def filter_mape_metrics(metrics_df: pd.DataFrame) -> pd.DataFrame:
    """Filter DataFrame to MAPE metrics only.

    Centralizes the repeated MAPE filtering pattern across charts and pages.

    Args:
        metrics_df: DataFrame with metric_name column.

    Returns:
        Filtered DataFrame containing only MAPE rows.
    """
    return metrics_df[metrics_df["metric_name"] == METRIC_MAPE].copy()


def filter_by_content_type(
    df: pd.DataFrame,
    volatility_df: pd.DataFrame,
    content_type: str,
    ad_unit_col: str = "ad_unit",
) -> pd.DataFrame:
    """Filter DataFrame by content type (stable vs event-driven).

    Uses coefficient of variation (CV) to classify ad units:
    - Stable: CV < VOLATILITY_CV_THRESHOLD (0.5)
    - Event-driven: CV >= VOLATILITY_CV_THRESHOLD (0.5)

    Args:
        df: DataFrame to filter (must have ad_unit column).
        volatility_df: DataFrame with ad_unit and cv columns from get_volatility_metrics().
        content_type: One of CONTENT_TYPE_ALL, CONTENT_TYPE_STABLE, CONTENT_TYPE_EVENT_DRIVEN.
        ad_unit_col: Name of the ad unit column in df.

    Returns:
        Filtered DataFrame. Returns copy if filtered, original if no filter applied.

    Raises:
        ValueError: If content_type is not a valid option.
    """
    valid_types = {CONTENT_TYPE_ALL, CONTENT_TYPE_STABLE, CONTENT_TYPE_EVENT_DRIVEN}
    if content_type not in valid_types:
        raise ValueError(f"Invalid content_type '{content_type}'. Must be one of {valid_types}")

    if content_type == CONTENT_TYPE_ALL:
        return df

    if volatility_df.empty or "cv" not in volatility_df.columns:
        return df

    threshold = settings.VOLATILITY_CV_THRESHOLD

    if content_type == CONTENT_TYPE_STABLE:
        stable_units = volatility_df[volatility_df["cv"] < threshold]["ad_unit"].tolist()
        return df[df[ad_unit_col].isin(stable_units)].copy()

    # content_type == CONTENT_TYPE_EVENT_DRIVEN (guaranteed by validation above)
    event_driven_units = volatility_df[volatility_df["cv"] >= threshold]["ad_unit"].tolist()
    return df[df[ad_unit_col].isin(event_driven_units)].copy()


def require_dataframe(
    df: pd.DataFrame,
    empty_message: str = "No data available.",
) -> bool:
    """Check if DataFrame has data and display info message if empty.

    Use this helper to eliminate repeated empty DataFrame checks across
    the codebase. Returns True if data exists, False if empty.

    Args:
        df: DataFrame to validate.
        empty_message: Message to display if DataFrame is empty.

    Returns:
        True if DataFrame has rows, False if empty.

    Example:
        >>> if not require_dataframe(df, "No metrics available."):
        ...     return
        >>> # Continue processing df...
    """
    if df.empty:
        st.info(empty_message)
        return False
    return True


def format_pivot_labels(
    pivot: pd.DataFrame,
    format_columns: bool = True,
    format_index: bool = True,
) -> pd.DataFrame:
    """Format pivot table column and index labels for display.

    Applies model name formatting to columns and ad unit formatting to index.
    Centralizes the repeated pivot formatting pattern across pages.

    Args:
        pivot: Pivot table DataFrame.
        format_columns: Whether to format column names as model names.
        format_index: Whether to format index as ad unit names.

    Returns:
        DataFrame with formatted labels (copy, not mutated).

    Example:
        >>> pivot = df.pivot(index="ad_unit", columns="model_name", values="mape")
        >>> formatted = format_pivot_labels(pivot)
    """
    result = pivot.copy()
    if format_columns:
        result.columns = result.columns.map(format_model_name)
    if format_index:
        result.index = result.index.map(format_ad_unit_name)
    return result


def reorder_pivot(
    df: pd.DataFrame,
    row_order: Sequence[str] | None = None,
    col_order: Sequence[str] | None = None,
) -> pd.DataFrame:
    """Reorder pivot table rows and columns by specified order.

    Filters to only include items that exist in the DataFrame, preserving
    the order specified while dropping items not in the data.

    Args:
        df: Pivot table DataFrame.
        row_order: Desired row index order. None to keep original.
        col_order: Desired column order. None to keep original.

    Returns:
        Reordered DataFrame copy. Always returns a copy for predictable semantics.

    Example:
        >>> pivot = df.pivot(index="model", columns="metric", values="value")
        >>> reorder_pivot(pivot, row_order=["model_a", "model_b"], col_order=["mape", "rmse"])
    """
    result = df.copy()

    if row_order is not None:
        valid_rows = [r for r in row_order if r in result.index]
        if valid_rows:
            result = result.loc[valid_rows]

    if col_order is not None:
        valid_cols = [c for c in col_order if c in result.columns]
        if valid_cols:
            result = result[valid_cols]

    return result


def apply_table_formatting(
    df: pd.DataFrame,
    column_formatters: dict[str, Callable[[str], str]],
    column_renames: dict[str, str],
    style_formats: dict[str, str],
) -> Styler:
    """Apply standard formatting pipeline to a DataFrame for display.

    Consolidates the repeated pattern of mapping column values, renaming
    columns, and applying style formats.

    Args:
        df: DataFrame to format (will be copied, not mutated).
        column_formatters: Dict mapping column name to formatter function.
        column_renames: Dict mapping old column names to display names.
        style_formats: Dict mapping column names to format strings for styling.

    Returns:
        Styled DataFrame ready for st.dataframe().

    Example:
        >>> styled = apply_table_formatting(
        ...     df,
        ...     column_formatters={"model_name": format_model_name, "ad_unit": format_ad_unit_name},
        ...     column_renames={"model_name": "Model", "ad_unit": "Ad Unit"},
        ...     style_formats={"mape": "{:.1f}%", "revenue": "${:,.0f}"},
        ... )
        >>> st.dataframe(styled, width="stretch")
    """
    display_df = df.copy()

    for col, formatter in column_formatters.items():
        if col in display_df.columns:
            display_df[col] = display_df[col].map(formatter)

    display_df = display_df.rename(columns=column_renames)
    return display_df.style.format(style_formats)


def fetch_with_fold_fallback(
    selected_fold: str,
    query_func: Callable[..., pd.DataFrame],
    caption_suffix: str = "",
) -> pd.DataFrame:
    """Display fold fallback caption and fetch data in one step.

    Consolidates the repeated pattern of resolving the detail fold with display
    followed by a query function.

    Args:
        selected_fold: User-selected fold (cross_fold_avg, fold_1, fold_2).
        query_func: Query function that takes fold as first argument.
        caption_suffix: Optional text for fallback caption.

    Returns:
        DataFrame from query function.

    Example:
        >>> df = fetch_with_fold_fallback(
        ...     selected_fold,
        ...     get_metrics_detail,
        ...     caption_suffix="MAPE heatmap",
        ... )
    """
    actual_fold = resolve_detail_fold(selected_fold, caption_suffix, display=True)
    return query_func(actual_fold)


def resolve_detail_fold(
    selected_fold: str,
    caption_suffix: str = "",
    display: bool = False,
) -> str:
    """Resolve fold name for per-ad-unit detail views with optional display.

    Per-ad-unit views (heatmaps, distributions, business impact) don't have
    cross_fold_avg data. This helper provides graceful degradation to the
    default detail fold with an explanatory caption.

    Args:
        selected_fold: User-selected fold (cross_fold_avg, fold_1, fold_2).
        caption_suffix: Text to replace "per-ad-unit breakdown" in caption.
        display: If True, show caption via st.caption when fallback occurs.

    Returns:
        Actual fold name to use for queries.

    Examples:
        No display (silent resolution):
        >>> actual = resolve_detail_fold("cross_fold_avg")
        'fold_2'

        With display:
        >>> actual = resolve_detail_fold("cross_fold_avg", display=True)
        # Shows caption and returns 'fold_2'

        With custom suffix:
        >>> actual = resolve_detail_fold("cross_fold_avg", "revenue impact", display=True)
        # Shows "Showing Fold 2 (Oct-Dec 2024) for revenue impact"
    """
    if selected_fold == FOLD_CROSS_AVG:
        label = FOLD_LABELS.get(DEFAULT_DETAIL_FOLD, DEFAULT_DETAIL_FOLD)
        suffix = caption_suffix if caption_suffix else "per-ad-unit breakdown"
        if display:
            st.caption(f"Showing {label} for {suffix}")
        return DEFAULT_DETAIL_FOLD
    return selected_fold


def render_page_header(title: str, intro: str) -> None:
    """Render standard page header with title and intro markdown.

    Consolidates the repeated st.title + st.markdown pattern across pages.

    Args:
        title: Page title text.
        intro: Markdown intro paragraph.
    """
    st.title(title)
    st.markdown(intro)


def init_page(title: str, intro: str) -> None:
    """Initialize page with standard setup: session state, title, intro.

    Consolidates the repeated init_session_state() + render_page_header()
    pattern that appears at the top of every page.

    Args:
        title: Page title text.
        intro: Markdown intro paragraph.
    """
    from app.utils.state import init_session_state
    init_session_state()
    render_page_header(title, intro)


def display_metric_columns(metrics: dict[str, str]) -> None:
    """Display metrics in equally-spaced columns.

    Args:
        metrics: Dict mapping label to pre-formatted value string.

    Example:
        >>> display_metric_columns({
        ...     "Total Risk": "$125,000",
        ...     "Max Risk": "$50,000",
        ...     "High Risk Items": "3",
        ... })
    """
    cols = st.columns(len(metrics))
    for col, (label, value) in zip(cols, metrics.items()):
        with col:
            st.metric(label, value)


def display_chart_or_warning(
    df: pd.DataFrame,
    required_column: str,
    chart_builder: Callable[[pd.DataFrame], "go.Figure"],
    warning_message: str = "No data available.",
) -> bool:
    """Display a Plotly chart or warning based on DataFrame validity.

    Consolidates the repeated pattern of checking DataFrame validity
    and either displaying a chart or a warning message.

    Args:
        df: DataFrame to validate and pass to chart builder.
        required_column: Column that must exist for valid display.
        chart_builder: Function that builds Plotly figure from DataFrame.
        warning_message: Message to display if data is invalid.

    Returns:
        True if chart was displayed, False if warning was shown.

    Example:
        >>> from app.components.charts import comparison_heatmap
        >>> displayed = display_chart_or_warning(
        ...     metrics_df,
        ...     required_column="ad_unit",
        ...     chart_builder=comparison_heatmap,
        ...     warning_message="No per-ad-unit metrics available.",
        ... )
    """
    if not df.empty and required_column in df.columns:
        fig = chart_builder(df)
        st.plotly_chart(fig, width="stretch")
        return True
    st.warning(warning_message)
    return False


def display_component_stats(
    df: pd.DataFrame,
    components: list[tuple[str, str, Callable[[pd.Series], float]]],
    value_format: str = "{:,.0f}",
) -> None:
    """Display component statistics in columns.

    Args:
        df: DataFrame containing component columns.
        components: List of (column_name, label, aggregation_fn) tuples.
        value_format: Format string for metric values (default: comma-separated integer).
            Use "{:.1f}" for decimals, "${:,.0f}" for currency, etc.

    Example:
        >>> display_component_stats(decomp_df, [
        ...     ("trend", "Trend Range", lambda s: s.max() - s.min()),
        ...     ("seasonal_weekly", "Weekly Amplitude", lambda s: s.max() - s.min()),
        ...     ("holiday_effect", "Max Holiday Effect", lambda s: s.abs().max()),
        ... ])
        >>> # With custom format:
        >>> display_component_stats(df, components, value_format="${:,.2f}")
    """
    # Filter to components that exist in the dataframe
    valid = [(col, label, fn) for col, label, fn in components if col in df.columns]

    if not valid:
        return

    cols = st.columns(len(valid))
    for col, (column, label, agg_fn) in zip(cols, valid):
        with col:
            value = agg_fn(df[column])
            st.metric(label, value_format.format(value))
