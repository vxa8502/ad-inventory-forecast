"""Plotly chart builders for the dashboard."""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from config import settings
from app.constants import (
    format_model_name,
    format_component_name,
    format_ad_unit_name,
    get_model_color,
    get_component_color,
    ACTUALS_COLOR,
    ANOMALY_COLOR,
    PLOTLY_TEMPLATE,
    CHART_HEIGHT_DEFAULT,
    CHART_HEIGHT_COMPACT,
    CHART_HEIGHT_DECOMPOSITION,
    CHART_HEIGHT_PER_ROW,
    CHART_HEIGHT_MIN,
    DECOMPOSITION_ROW_HEIGHTS,
    DECOMPOSITION_VERTICAL_SPACING,
    LEGEND_TOP_LEFT,
    LEGEND_OUTSIDE_RIGHT,
    RANGE_BAND_COLOR,
    DEFAULT_SHOW_COMPONENTS,
)
from app.utils.helpers import build_lookup_dict, filter_mape_metrics, to_iso_date_str


@dataclass
class ForecastChartOptions:
    """Configuration options for forecast chart rendering.

    Groups optional parameters to reduce function signature complexity.

    Attributes:
        show_ci: Whether to display confidence interval bands.
        anomalies_df: DataFrame with detected anomalies to annotate.
        known_events: List of (date, description, category) tuples for events.
    """

    show_ci: bool = True
    anomalies_df: pd.DataFrame | None = None
    known_events: list[tuple[str, str, str]] = field(default_factory=list)


def _apply_standard_layout(
    fig: go.Figure,
    title: str,
    height: int = CHART_HEIGHT_DEFAULT,
    xaxis_title: str | None = None,
    yaxis_title: str | None = None,
    **kwargs,
) -> None:
    """Apply standard layout configuration to a Plotly figure.

    Args:
        fig: Plotly figure to configure.
        title: Chart title.
        height: Chart height in pixels.
        xaxis_title: Optional x-axis label.
        yaxis_title: Optional y-axis label.
        **kwargs: Additional layout options passed to update_layout.
    """
    layout_config = {
        "title": title,
        "template": PLOTLY_TEMPLATE,
        "height": height,
        **kwargs,
    }
    fig.update_layout(**layout_config)

    if xaxis_title:
        fig.update_xaxes(title_text=xaxis_title)
    if yaxis_title:
        fig.update_yaxes(title_text=yaxis_title)


def forecast_chart(
    actuals_df: pd.DataFrame,
    forecasts_df: pd.DataFrame,
    ad_unit: str,
    options: ForecastChartOptions | None = None,
) -> go.Figure:
    """Build forecast comparison chart for a single ad unit.

    Args:
        actuals_df: DataFrame with date, ad_unit, daily_impressions.
        forecasts_df: DataFrame with date, ad_unit, model_name, forecast,
            lower_bound, upper_bound.
        ad_unit: Ad unit to visualize.
        options: Configuration options for chart rendering.

    Returns:
        Plotly figure object.
    """
    if options is None:
        options = ForecastChartOptions()

    fig = go.Figure()

    # Filter to ad unit
    actuals = actuals_df[actuals_df["ad_unit"] == ad_unit]
    forecasts = forecasts_df[forecasts_df["ad_unit"] == ad_unit]

    # Add actuals line
    if not actuals.empty:
        _add_actuals_trace(fig, actuals["date"], actuals["daily_impressions"])

    # Add forecast lines for each model
    for model in forecasts["model_name"].unique():
        model_data = forecasts[forecasts["model_name"] == model]
        color = get_model_color(model)

        # Confidence interval band
        if options.show_ci and "lower_bound" in model_data.columns:
            _add_confidence_band_trace(
                fig,
                model_data["date"].values,
                model_data["upper_bound"].values,
                model_data["lower_bound"].values,
                color=color,
            )

        # Forecast line
        fig.add_trace(go.Scatter(
            x=model_data["date"],
            y=model_data["forecast"],
            mode="lines",
            name=format_model_name(model),
            line={"color": color, "width": 2, "dash": "dash"},
            hovertemplate=f"Date: %{{x}}<br>Forecast: %{{y:,.0f}}<extra>{format_model_name(model)}</extra>",
        ))

    # Add anomaly annotations
    _add_anomaly_annotations(fig, options.anomalies_df, options.known_events or None)

    _apply_standard_layout(
        fig,
        title=f"Forecast Comparison: {format_ad_unit_name(ad_unit)}",
        xaxis_title="Date",
        yaxis_title="Daily Impressions",
        hovermode="x unified",
        legend=LEGEND_TOP_LEFT,
    )

    return fig


def _add_anomaly_annotations(
    fig: go.Figure,
    anomalies_df: pd.DataFrame | None,
    known_events: list[tuple[str, str, str]] | None,
) -> None:
    """Add vertical lines and annotations for anomalies and known events.

    Args:
        fig: Plotly figure to annotate.
        anomalies_df: DataFrame with date column for detected anomalies.
        known_events: List of (date, description, category) tuples for known events.
    """
    # Build event lookup for annotation text
    event_lookup: dict[str, str] = {}
    if known_events:
        for date_str, description, _category in known_events:
            event_lookup[date_str] = description

    # Add vertical lines for anomalies
    if anomalies_df is not None and not anomalies_df.empty:
        for _, row in anomalies_df.iterrows():
            date_val = row["date"]
            date_str = to_iso_date_str(date_val)

            # Get event description if available
            annotation_text = event_lookup.get(date_str, "Anomaly")

            # Use add_shape + add_annotation separately to avoid Plotly datetime bug
            fig.add_shape(
                type="line",
                x0=date_val,
                x1=date_val,
                y0=0,
                y1=1,
                yref="paper",
                line=dict(width=1, dash="dot", color=ANOMALY_COLOR),
                opacity=0.7,
            )
            fig.add_annotation(
                x=date_val,
                y=1,
                yref="paper",
                text=annotation_text,
                showarrow=False,
                font=dict(size=9, color=ANOMALY_COLOR),
                yshift=10,
            )


def comparison_heatmap(metrics_df: pd.DataFrame) -> go.Figure:
    """Build MAPE heatmap (ad_unit x model).

    Args:
        metrics_df: DataFrame with model_name, ad_unit, metric_name, metric_value.

    Returns:
        Plotly figure object.
    """
    # Filter to MAPE only and pivot
    mape_df = filter_mape_metrics(metrics_df)

    if mape_df.empty:
        return _empty_figure("No MAPE data available")

    pivot = mape_df.pivot(
        index="ad_unit",
        columns="model_name",
        values="metric_value",
    )

    # Reorder columns; preserve NaN for missing data (don't fill with 0)
    col_order = [m for m in settings.MODEL_NAMES if m in pivot.columns]
    pivot = pivot[col_order]

    # Build text labels for heatmap cells (N/A for missing data)
    text_labels = [
        [f"{val:.1f}%" if pd.notna(val) else "N/A" for val in row]
        for row in pivot.values
    ]

    # Replace NaN with -1 for colorscale (will be gray via zmin handling)
    z_values = pivot.fillna(-1).values

    fig = go.Figure(data=go.Heatmap(
        z=z_values,
        x=[format_model_name(m) for m in pivot.columns],
        y=pivot.index,
        colorscale="RdYlGn_r",
        zmin=0,  # Actual MAPE starts at 0; -1 (missing) renders as darkest
        text=text_labels,
        texttemplate="%{text}",
        textfont={"size": 10},
        hovertemplate="Ad Unit: %{y}<br>Model: %{x}<br>MAPE: %{text}<extra></extra>",
        colorbar={"title": "MAPE %"},
    ))

    _apply_standard_layout(
        fig,
        title="MAPE by Ad Unit and Model",
        height=max(CHART_HEIGHT_MIN, len(pivot) * CHART_HEIGHT_PER_ROW),
        xaxis_title="Model",
        yaxis_title="Ad Unit",
    )

    return fig


def mape_boxplot(metrics_df: pd.DataFrame) -> go.Figure:
    """Build box plot of MAPE distribution per model.

    Args:
        metrics_df: DataFrame with model_name, ad_unit, metric_name, metric_value.

    Returns:
        Plotly figure object.
    """
    mape_df = filter_mape_metrics(metrics_df)

    if mape_df.empty:
        return _empty_figure("No MAPE data available")

    fig = go.Figure()

    for model in settings.MODEL_NAMES:
        model_data = mape_df[mape_df["model_name"] == model]
        if not model_data.empty:
            fig.add_trace(go.Box(
                y=model_data["metric_value"],
                name=format_model_name(model),
                marker_color=get_model_color(model),
                boxpoints="outliers",
            ))

    _apply_standard_layout(
        fig,
        title="MAPE Distribution by Model",
        height=CHART_HEIGHT_COMPACT,
        yaxis_title="MAPE %",
        showlegend=False,
    )

    return fig


def residuals_histogram(residuals_df: pd.DataFrame) -> go.Figure:
    """Build faceted histogram of forecast residuals per model.

    Residuals = forecast - actual. Positive = overforecast, negative = underforecast.
    A well-calibrated model should have residuals centered near zero.

    Args:
        residuals_df: DataFrame with model_name, residual columns.

    Returns:
        Plotly figure with one histogram per model in faceted layout.
    """
    if residuals_df.empty:
        return _empty_figure("No residual data available")

    models = [m for m in settings.MODEL_NAMES if m in residuals_df["model_name"].unique()]

    if not models:
        return _empty_figure("No residual data for selected models")

    fig = make_subplots(
        rows=1,
        cols=len(models),
        subplot_titles=[format_model_name(m) for m in models],
        shared_yaxes=True,
    )

    for i, model in enumerate(models, 1):
        model_data = residuals_df[residuals_df["model_name"] == model]
        color = get_model_color(model)

        fig.add_trace(
            go.Histogram(
                x=model_data["residual"],
                name=format_model_name(model),
                marker_color=color,
                opacity=0.75,
                showlegend=False,
                hovertemplate="Residual: %{x:,.0f}<br>Count: %{y}<extra></extra>",
            ),
            row=1,
            col=i,
        )

        # Add vertical line at zero
        fig.add_vline(
            x=0,
            line_width=2,
            line_dash="dash",
            line_color="white",
            opacity=0.5,
            row=1,
            col=i,
        )

    _apply_standard_layout(
        fig,
        title="Forecast Residuals Distribution (Forecast - Actual)",
        height=CHART_HEIGHT_COMPACT,
    )

    fig.update_xaxes(title_text="Residual (Impressions)")
    fig.update_yaxes(title_text="Frequency", col=1)

    return fig


def decomposition_chart(
    decomp_df: pd.DataFrame,
    ad_unit: str,
    show_components: dict[str, bool] | None = None,
    holidays_df: pd.DataFrame | None = None,
    holiday_effects_df: pd.DataFrame | None = None,
) -> go.Figure:
    """Build stacked area chart for ARIMA decomposition.

    Args:
        decomp_df: DataFrame with date, trend, seasonal_weekly,
            seasonal_yearly, holiday_effect, step_change.
        ad_unit: Ad unit name for title.
        show_components: Dict mapping component name to visibility.
        holidays_df: Optional DataFrame with holiday_date, holiday_name for annotations.
        holiday_effects_df: Optional DataFrame with date, holiday_effect for effect magnitude.

    Returns:
        Plotly figure object.
    """
    if decomp_df.empty:
        return _empty_figure("No decomposition data available")

    if show_components is None:
        show_components = DEFAULT_SHOW_COMPONENTS.copy()

    fig = make_subplots(
        rows=2,
        cols=1,
        row_heights=DECOMPOSITION_ROW_HEIGHTS,
        shared_xaxes=True,
        vertical_spacing=DECOMPOSITION_VERTICAL_SPACING,
        subplot_titles=("Components", "Step Change / Residual"),
    )

    # Stacked area for components
    for component, visible in show_components.items():
        if visible and component in decomp_df.columns:
            fig.add_trace(go.Scatter(
                x=decomp_df["date"],
                y=decomp_df[component],
                mode="lines",
                name=format_component_name(component),
                line={"color": get_component_color(component)},
                stackgroup="components",
            ), row=1, col=1)

    # Step change as separate line
    if "step_change" in decomp_df.columns:
        fig.add_trace(go.Scatter(
            x=decomp_df["date"],
            y=decomp_df["step_change"],
            mode="lines",
            name="Step Change",
            line={"color": get_component_color("step_change"), "width": 1},
        ), row=2, col=1)

    # Add holiday annotations as vertical lines
    _add_holiday_annotations(fig, holidays_df, holiday_effects_df, decomp_df)

    _apply_standard_layout(
        fig,
        title=f"ARIMA Decomposition: {format_ad_unit_name(ad_unit)}",
        height=CHART_HEIGHT_DECOMPOSITION,
        legend=LEGEND_OUTSIDE_RIGHT,
    )

    # Subplot-specific axis labels
    fig.update_xaxes(title_text="Date", row=2, col=1)
    fig.update_yaxes(title_text="Impressions", row=1, col=1)
    fig.update_yaxes(title_text="Step Change", row=2, col=1)

    return fig


def _add_holiday_annotations(
    fig: go.Figure,
    holidays_df: pd.DataFrame | None,
    holiday_effects_df: pd.DataFrame | None,
    decomp_df: pd.DataFrame,
) -> None:
    """Add vertical dashed lines for US holidays with effect magnitude.

    Args:
        fig: Plotly figure to annotate.
        holidays_df: DataFrame with holiday_date, holiday_name columns.
        holiday_effects_df: DataFrame with date, holiday_effect columns.
        decomp_df: Decomposition DataFrame to get date range.
    """
    if holidays_df is None or holidays_df.empty:
        return

    # Build effect lookup for tooltip
    effect_lookup: dict[str, float] = build_lookup_dict(
        holiday_effects_df, "date", "holiday_effect", key_transform=to_iso_date_str
    )

    # Get date range from decomp data (convert to pandas Timestamp for comparison)
    date_min = pd.Timestamp(decomp_df["date"].min())
    date_max = pd.Timestamp(decomp_df["date"].max())

    # Only show major holidays to avoid clutter
    major_holidays = holidays_df[holidays_df["is_major"]]

    for _, row in major_holidays.iterrows():
        holiday_ts = pd.Timestamp(row["holiday_date"])

        # Skip if outside decomposition date range
        if holiday_ts < date_min or holiday_ts > date_max:
            continue

        holiday_name = row["holiday_name"]
        date_str = to_iso_date_str(holiday_ts)

        # Get effect magnitude if available (check both None and NaN)
        effect = effect_lookup.get(date_str)
        has_effect = effect is not None and pd.notna(effect)
        annotation_text = f"{holiday_name} ({effect:+,.0f})" if has_effect else holiday_name

        # Convert to ISO string for Plotly compatibility (avoids datetime arithmetic bugs)
        holiday_date_str = to_iso_date_str(holiday_ts)

        # Use add_shape instead of add_vline to avoid Plotly datetime annotation bug
        fig.add_shape(
            type="line",
            x0=holiday_date_str,
            x1=holiday_date_str,
            y0=0,
            y1=1,
            yref="y domain",
            line=dict(
                width=1,
                dash="dash",
                color=get_component_color("holiday_effect"),
            ),
            opacity=0.6,
            row=1,
            col=1,
        )

        # Add annotation separately
        fig.add_annotation(
            x=holiday_date_str,
            y=1,
            yref="y domain",
            text=annotation_text,
            showarrow=False,
            font=dict(size=8, color=get_component_color("holiday_effect")),
            textangle=-45,
            xanchor="left",
            yanchor="bottom",
            row=1,
            col=1,
        )


def anomaly_chart(
    data_df: pd.DataFrame,
    ad_unit: str,
    threshold: float = 0.95,
) -> go.Figure:
    """Build time series chart with anomaly markers.

    Args:
        data_df: DataFrame with date, daily_impressions, is_anomaly,
            lower_bound, upper_bound, anomaly_probability.
        ad_unit: Ad unit name for title.
        threshold: Probability threshold for highlighting.

    Returns:
        Plotly figure object.
    """
    if data_df.empty:
        return _empty_figure("No anomaly data available")

    fig = go.Figure()

    # Expected range band
    if "lower_bound" in data_df.columns and "upper_bound" in data_df.columns:
        _add_confidence_band_trace(
            fig,
            data_df["date"].values,
            data_df["upper_bound"].values,
            data_df["lower_bound"].values,
            name="Expected Range",
        )

    # Actual values line
    _add_actuals_trace(fig, data_df["date"], data_df["daily_impressions"])

    # Anomaly markers
    anomalies = data_df[
        data_df["is_anomaly"] | (data_df["anomaly_probability"] >= threshold)
    ]

    if not anomalies.empty:
        fig.add_trace(go.Scatter(
            x=anomalies["date"],
            y=anomalies["daily_impressions"],
            mode="markers",
            name="Anomalies",
            marker={
                "color": ANOMALY_COLOR,
                "size": 12,
                "symbol": "x",
                "line": {"width": 2, "color": ANOMALY_COLOR},
            },
            hovertemplate=(
                "Date: %{x}<br>"
                "Impressions: %{y:,.0f}<br>"
                "Probability: %{customdata:.1%}<extra>Anomaly</extra>"
            ),
            customdata=anomalies["anomaly_probability"],
        ))

    _apply_standard_layout(
        fig,
        title=f"Anomaly Detection: {format_ad_unit_name(ad_unit)}",
        xaxis_title="Date",
        yaxis_title="Daily Impressions",
        hovermode="x unified",
    )

    return fig


def _hex_to_rgba(hex_color: str, alpha: float) -> tuple[int, int, int, float]:
    """Convert hex color (e.g., #2E86AB) to RGBA tuple.

    Args:
        hex_color: Hex color code with or without leading "#".
        alpha: Alpha/opacity value (0.0 = transparent, 1.0 = opaque).

    Returns:
        Tuple of (R, G, B, alpha) with RGB as 0-255 integers.

    Example:
        >>> _hex_to_rgba("#2E86AB", 0.5)
        (46, 134, 171, 0.5)
    """
    r, g, b = bytes.fromhex(hex_color.lstrip("#"))
    return (r, g, b, alpha)


def _add_actuals_trace(
    fig: go.Figure,
    dates: pd.Series,
    values: pd.Series,
    name: str = "Actuals",
) -> None:
    """Add actuals line trace to a figure.

    Args:
        fig: Plotly figure to add trace to.
        dates: Date series for x-axis.
        values: Impression values for y-axis.
        name: Legend name for the trace.
    """
    fig.add_trace(go.Scatter(
        x=dates,
        y=values,
        mode="lines",
        name=name,
        line={"color": ACTUALS_COLOR, "width": 2},
        hovertemplate="Date: %{x}<br>Impressions: %{y:,.0f}<extra></extra>",
    ))


def _add_confidence_band_trace(
    fig: go.Figure,
    dates: np.ndarray,
    upper: np.ndarray,
    lower: np.ndarray,
    color: str | None = None,
    name: str | None = None,
    alpha: float = 0.2,
) -> None:
    """Add confidence interval band trace to a figure.

    Args:
        fig: Plotly figure to add trace to.
        dates: Date array for x-axis.
        upper: Upper bound values.
        lower: Lower bound values.
        color: Hex color for fill (uses RANGE_BAND_COLOR if None).
        name: Optional legend name (hidden if None).
        alpha: Opacity for fill color.
    """
    fillcolor = f"rgba{_hex_to_rgba(color, alpha)}" if color else RANGE_BAND_COLOR

    fig.add_trace(go.Scatter(
        x=np.concatenate([dates, dates[::-1]]),
        y=np.concatenate([upper, lower[::-1]]),
        fill="toself",
        fillcolor=fillcolor,
        line={"width": 0},
        showlegend=name is not None,
        name=name,
        hoverinfo="skip",
    ))


def _empty_figure(message: str) -> go.Figure:
    """Create an empty figure with a centered message.

    Args:
        message: Text to display in the empty figure.

    Returns:
        Plotly figure with annotation.
    """
    fig = go.Figure()
    fig.add_annotation(
        text=message,
        xref="paper",
        yref="paper",
        x=0.5,
        y=0.5,
        showarrow=False,
        font={"size": 14},
    )
    fig.update_layout(template=PLOTLY_TEMPLATE)
    return fig
