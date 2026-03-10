"""Shared display name mappings and constants for the dashboard."""

from __future__ import annotations

__all__ = [
    # Display name mappings
    "MODEL_DISPLAY_NAMES",
    "COMPONENT_DISPLAY_NAMES",
    "METRIC_DISPLAY_NAMES",
    "METRIC_INTERNAL_NAMES",
    # Metric constants
    "METRIC_MAPE",
    "METRIC_RMSE",
    "METRIC_MAE",
    "METRIC_MASE",
    "METRIC_COVERAGE",
    "MAXIMIZE_METRICS",
    # Column mappings
    "COLUMNS_BUSINESS_IMPACT",
    "COLUMNS_ANOMALIES",
    "COLUMNS_FORECASTS",
    "COLUMNS_METRICS",
    "COLUMNS_DECOMPOSITION",
    "FORMAT_BUSINESS_IMPACT",
    "FORMAT_ANOMALIES",
    # Fold constants
    "FOLD_CROSS_AVG",
    "FOLD_1",
    "FOLD_2",
    "FOLD_LABELS",
    "DEFAULT_DETAIL_FOLD",
    # Content type constants
    "CONTENT_TYPE_ALL",
    "CONTENT_TYPE_STABLE",
    "CONTENT_TYPE_EVENT_DRIVEN",
    "CONTENT_TYPE_LABELS",
    # Filter defaults
    "DEFAULT_FILTER_AD_UNITS",
    "DEFAULT_ANOMALY_THRESHOLD",
    "ANOMALY_THRESHOLD_MIN",
    "ANOMALY_THRESHOLD_MAX",
    "ANOMALY_THRESHOLD_DEFAULT",
    # Cache and display
    "QUERY_CACHE_TTL",
    "PLOTLY_TEMPLATE",
    # Business impact
    "DEFAULT_CPM",
    "HIGH_RISK_THRESHOLD",
    # Colors
    "MODEL_COLORS",
    "COMPONENT_COLORS",
    "ACTUALS_COLOR",
    "ANOMALY_COLOR",
    "DEFAULT_CHART_COLOR",
    "HIGHLIGHT_BEST_COLOR",
    "RANGE_BAND_COLOR",
    "RANGE_BAND_ALPHA",
    # Chart dimensions
    "CHART_HEIGHT_DEFAULT",
    "CHART_HEIGHT_COMPACT",
    "CHART_HEIGHT_DECOMPOSITION",
    "CHART_HEIGHT_PER_ROW",
    "CHART_HEIGHT_MIN",
    "DECOMPOSITION_ROW_HEIGHTS",
    "DECOMPOSITION_VERTICAL_SPACING",
    "LEGEND_TOP_LEFT",
    "LEGEND_OUTSIDE_RIGHT",
    # Classes
    "WidgetLabels",
    "WidgetKeys",
    "ValidationMessages",
    "TabNames",
    # Functions
    "format_model_name",
    "format_component_name",
    "format_metric_name",
    "format_ad_unit_name",
    "get_model_color",
    "get_component_color",
    # Hypothesis and step change
    "DEFAULT_SHOW_COMPONENTS",
    "HYPOTHESIS_VALIDATION",
    "ARTICLES_WITH_STEP_CHANGES",
    "STEP_CHANGE_STATS",
]

import os

# Model display names (internal -> human-readable)
MODEL_DISPLAY_NAMES: dict[str, str] = {
    "timesfm_2_5": "TimesFM 2.5",
    "arima_plus": "ARIMA+",
    "arima_plus_xreg": "ARIMA+ XREG",
}

# Component display names for decomposition charts
COMPONENT_DISPLAY_NAMES: dict[str, str] = {
    "trend": "Trend",
    "seasonal_weekly": "Weekly Seasonality",
    "seasonal_yearly": "Yearly Seasonality",
    "holiday_effect": "Holiday Effect",
    "step_change": "Step Change",
}

# Metric display names
# Metric name constants (use these instead of magic strings)
METRIC_MAPE = "mape"
METRIC_RMSE = "rmse"
METRIC_MAE = "mae"
METRIC_MASE = "mase"
METRIC_COVERAGE = "coverage"

METRIC_DISPLAY_NAMES: dict[str, str] = {
    METRIC_MAPE: "MAPE %",
    METRIC_RMSE: "RMSE",
    METRIC_MAE: "MAE",
    METRIC_MASE: "MASE",
    METRIC_COVERAGE: "Coverage %",
}

# Reverse lookup: display name -> internal name (for table highlighting)
METRIC_INTERNAL_NAMES: dict[str, str] = {v: k for k, v in METRIC_DISPLAY_NAMES.items()}

# Metrics where higher values are better (used for table highlighting).
# All other metrics are assumed to be "lower is better" (e.g., MAPE, RMSE).
MAXIMIZE_METRICS: frozenset[str] = frozenset({METRIC_COVERAGE})

# Column rename mappings for table display (internal -> display name)
COLUMNS_BUSINESS_IMPACT: dict[str, str] = {
    "model_name": "Model",
    "ad_unit": "Ad Unit",
    "avg_daily_impressions": "Avg Daily Impressions",
    METRIC_MAPE: "MAPE %",
    "cpm": "CPM ($)",
    "daily_revenue_at_risk": "Daily Risk ($)",
    "annual_revenue_at_risk": "Annual Risk ($)",
}

COLUMNS_ANOMALIES: dict[str, str] = {
    "date": "Date",
    "ad_unit": "Ad Unit",
    "daily_impressions": "Impressions",
    "anomaly_probability": "Probability",
    "event": "Known Event",
    "category": "Category",
    "forecastability": "Forecastability",
}

COLUMNS_FORECASTS: dict[str, str] = {
    "date": "Date",
    "ad_unit": "Ad Unit",
    "model_name": "Model",
    "forecast": "Forecast",
    "lower_bound": "Lower Bound",
    "upper_bound": "Upper Bound",
    "daily_impressions": "Actual",
}

COLUMNS_METRICS: dict[str, str] = {
    "model_name": "Model",
    "ad_unit": "Ad Unit",
    "fold_name": "Fold",
    "metric_name": "Metric",
    "metric_value": "Value",
}

COLUMNS_DECOMPOSITION: dict[str, str] = {
    "date": "Date",
    "ad_unit": "Ad Unit",
    "trend": "Trend",
    "seasonal_weekly": "Weekly",
    "seasonal_yearly": "Yearly",
    "holiday_effect": "Holiday",
    "step_change": "Step Change",
}

# Column format strings (keyed by display name)
FORMAT_BUSINESS_IMPACT: dict[str, str] = {
    "Avg Daily Impressions": "{:,.0f}",
    "MAPE %": "{:.1f}%",
    "CPM ($)": "${:.2f}",
    "Daily Risk ($)": "${:,.2f}",
    "Annual Risk ($)": "${:,.0f}",
}

FORMAT_ANOMALIES: dict[str, str] = {
    "Impressions": "{:,.0f}",
    "Probability": "{:.1%}",
}

# Fold name constants (use these instead of magic strings)
FOLD_CROSS_AVG = "cross_fold_avg"
FOLD_1 = "fold_1"
FOLD_2 = "fold_2"

# Content type constants for volatility-based filtering
CONTENT_TYPE_ALL = "all"
CONTENT_TYPE_STABLE = "stable"
CONTENT_TYPE_EVENT_DRIVEN = "event_driven"

CONTENT_TYPE_LABELS: dict[str, str] = {
    CONTENT_TYPE_ALL: "All Content",
    CONTENT_TYPE_STABLE: "Stable (CV < 0.5)",
    CONTENT_TYPE_EVENT_DRIVEN: "Event-Driven (CV >= 0.5)",
}

# Fold display labels
FOLD_LABELS: dict[str, str] = {
    FOLD_CROSS_AVG: "Cross-Fold Average",
    FOLD_1: "Fold 1 (Jul-Sep 2024)",
    FOLD_2: "Fold 2 (Oct-Dec 2024)",
}

# Default fallback fold for detail views that don't support cross_fold_avg
DEFAULT_DETAIL_FOLD = FOLD_2

# Default ad units for filter widgets
DEFAULT_FILTER_AD_UNITS: list[str] = ["Taylor_Swift", "Bitcoin", "NFL"]

# Default anomaly threshold
DEFAULT_ANOMALY_THRESHOLD = 0.95

# Query cache TTL in seconds.
# 1 hour balances data freshness (hourly changes acceptable for this dashboard)
# vs query cost (avoid re-querying BigQuery on every interaction).
# Clear manually via sidebar Refresh button or programmatically via clear_query_cache().
# Override via QUERY_CACHE_TTL_SECONDS env var (e.g., shorter TTL for development).
QUERY_CACHE_TTL = int(os.getenv("QUERY_CACHE_TTL_SECONDS", "3600"))

# Plotly theme
PLOTLY_TEMPLATE = "plotly_dark"

# Business impact calculations
# Cost per mille (thousand impressions) - industry benchmark for display inventory
DEFAULT_CPM: float = 5.50

# Base color palette (single source of truth for all colors)
class _Colors:
    """Internal color palette. Use the public constants below."""

    BLUE: str = "#2E86AB"
    RED: str = "#E94F37"
    GREEN: str = "#44AF69"
    PINK: str = "#F8333C"
    PURPLE: str = "#A23B72"
    WHITE: str = "#FAFAFA"
    CORAL: str = "#FF6B6B"
    GRAY: str = "#888888"


# Color palette for model lines
MODEL_COLORS: dict[str, str] = {
    "timesfm_2_5": _Colors.BLUE,
    "arima_plus": _Colors.RED,
    "arima_plus_xreg": _Colors.GREEN,
}

# Color palette for decomposition components
COMPONENT_COLORS: dict[str, str] = {
    "trend": _Colors.BLUE,
    "seasonal_weekly": _Colors.RED,
    "seasonal_yearly": _Colors.GREEN,
    "holiday_effect": _Colors.PINK,
    "step_change": _Colors.PURPLE,
}

# Chart colors
ACTUALS_COLOR = _Colors.WHITE
ANOMALY_COLOR = _Colors.CORAL
DEFAULT_CHART_COLOR = _Colors.GRAY
HIGHLIGHT_BEST_COLOR = _Colors.GREEN

# Chart dimensions (in pixels).
# Desktop viewport ~800px height minus header/margins leaves ~450px for charts.
# COMPACT: Slightly less for boxplots/distributions that don't need CI bands.
# DECOMPOSITION: Extra height for 2-row subplot layout (components + step change).
# PER_ROW/MIN: Dynamic height formula for heatmaps: max(MIN, num_rows * PER_ROW).
CHART_HEIGHT_DEFAULT = 450
CHART_HEIGHT_COMPACT = 400
CHART_HEIGHT_DECOMPOSITION = 600
CHART_HEIGHT_PER_ROW = 25
CHART_HEIGHT_MIN = 400

# Decomposition chart layout
DECOMPOSITION_ROW_HEIGHTS: list[float] = [0.7, 0.3]
DECOMPOSITION_VERTICAL_SPACING = 0.08

# Legend positioning presets (Plotly format)
def _legend_position(x: float) -> dict[str, str | float]:
    """Build legend position dict with specified x value.

    DRY factory: all legend positions share yanchor, y, xanchor.
    Only x value differs between presets.
    """
    return {"yanchor": "top", "y": 0.99, "xanchor": "left", "x": x}


LEGEND_TOP_LEFT: dict[str, str | float] = _legend_position(0.01)
LEGEND_OUTSIDE_RIGHT: dict[str, str | float] = _legend_position(1.02)

# Confidence interval / range band color
RANGE_BAND_ALPHA = 0.2
RANGE_BAND_COLOR = "rgba(46, 134, 171, 0.2)"

# Filter slider ranges
ANOMALY_THRESHOLD_MIN = 0.90
ANOMALY_THRESHOLD_MAX = 0.99
ANOMALY_THRESHOLD_DEFAULT = DEFAULT_ANOMALY_THRESHOLD

# Business impact thresholds
HIGH_RISK_THRESHOLD = 100_000  # annual revenue at risk in dollars


class WidgetLabels:
    """Centralized widget label strings for consistent UI text.

    Keeps user-facing strings in one place for easy updates and i18n readiness.

    Usage:
        >>> from app.constants import WidgetLabels
        >>> st.multiselect(WidgetLabels.AD_UNITS, options=...)
    """

    AD_UNITS = "Ad Units"
    AD_UNIT = "Ad Unit"
    MODELS = "Models"
    VALIDATION_FOLD = "Validation Fold"
    FORECAST_HORIZON = "Forecast Horizon (days)"
    ANOMALY_THRESHOLD = "Anomaly Threshold"
    CONTENT_TYPE = "Content Type"
    SHOW_CI = "Show Confidence Intervals"
    COMPONENTS = "Components"
    TREND = "Trend"
    WEEKLY_SEASONALITY = "Weekly Seasonality"
    YEARLY_SEASONALITY = "Yearly Seasonality"
    HOLIDAY_EFFECT = "Holiday Effect"
    REFRESH_DATA = "Refresh Data"


class WidgetKeys:
    """Centralized widget key constants for type safety and consistency.

    Naming convention:
    - PAGE_SELECTED_ITEM for selection widgets (multiselect, selectbox, radio)
    - PAGE_SHOW_ITEM for toggles/checkboxes
    - PAGE_ITEM_VALUE for sliders and numeric inputs

    Usage:
        >>> from app.constants import WidgetKeys
        >>> ad_unit_selector(key=WidgetKeys.FORECAST_SELECTED_AD_UNITS)
    """

    # Forecast Explorer page
    FORECAST_SELECTED_AD_UNITS = "forecast_selected_ad_units"
    FORECAST_SELECTED_MODELS = "forecast_selected_models"
    FORECAST_SELECTED_FOLD = "forecast_selected_fold"
    FORECAST_SHOW_CI = "forecast_show_ci"
    FORECAST_HORIZON = "forecast_horizon"

    # Model Comparison page
    COMPARISON_SELECTED_FOLD = "comparison_selected_fold"
    COMPARISON_CONTENT_TYPE = "comparison_content_type"

    # Decomposition page
    DECOMP_SELECTED_AD_UNIT = "decomp_selected_ad_unit"
    DECOMP_SELECTED_FOLD = "decomp_selected_fold"
    DECOMP_SHOW_TREND = "decomp_show_trend"
    DECOMP_SHOW_WEEKLY = "decomp_show_weekly"
    DECOMP_SHOW_YEARLY = "decomp_show_yearly"
    DECOMP_SHOW_HOLIDAY = "decomp_show_holiday"

    # Anomaly Detection page
    ANOMALY_SELECTED_AD_UNITS = "anomaly_selected_ad_units"
    ANOMALY_SELECTED_FOLD = "anomaly_selected_fold"
    ANOMALY_THRESHOLD_VALUE = "anomaly_threshold_value"


class ValidationMessages:
    """Centralized validation messages for consistent user feedback.

    Usage:
        >>> from app.constants import ValidationMessages
        >>> st.warning(ValidationMessages.SELECT_AD_UNIT)
    """

    SELECT_AD_UNIT = "Select at least one ad unit from the sidebar."
    SELECT_MODEL = "Select at least one model from the sidebar."
    NO_DATA_FOR_FOLD = "No data available for this fold."
    NO_METRICS_FOR_FOLD = "No metrics data available for this fold."
    NO_METRICS_FOR_FILTERS = "No metrics found for selected filters."
    NO_MAPE_METRICS = "No MAPE metrics available."
    NO_DECOMPOSITION_DATA = "No decomposition data available for {ad_unit} in {fold}."
    NO_ANOMALY_DATA = "No anomaly data available for {ad_unit}."
    NO_ANOMALIES_ABOVE_THRESHOLD = "No anomalies detected above the selected threshold."
    NO_BUSINESS_IMPACT = "No business impact data available for this fold."
    NO_PER_UNIT_METRICS = "No per-ad-unit metrics available for this fold."


class TabNames:
    """Centralized tab name constants for consistent UI labeling.

    Usage:
        >>> from app.constants import TabNames
        >>> st.tabs(TabNames.COMPARISON_TABS)
    """

    # Model Comparison page tabs
    COMPARISON_HEADLINE = "Headline Metrics"
    COMPARISON_HEATMAP = "MAPE Heatmap"
    COMPARISON_DISTRIBUTION = "Distribution"
    COMPARISON_IMPACT = "Business Impact"

    # Ordered list for st.tabs()
    COMPARISON_TABS: list[str] = [
        COMPARISON_HEADLINE,
        COMPARISON_HEATMAP,
        COMPARISON_DISTRIBUTION,
        COMPARISON_IMPACT,
    ]


def format_model_name(model: str) -> str:
    """Format model name for display.

    Args:
        model: Internal model identifier.

    Returns:
        Human-readable model name.
    """
    return MODEL_DISPLAY_NAMES.get(model, model)


def format_component_name(component: str) -> str:
    """Format decomposition component name for display.

    Args:
        component: Internal component identifier.

    Returns:
        Human-readable component name.
    """
    return COMPONENT_DISPLAY_NAMES.get(component, component)


def format_metric_name(metric: str) -> str:
    """Format metric name for display.

    Args:
        metric: Internal metric identifier.

    Returns:
        Human-readable metric name.
    """
    return METRIC_DISPLAY_NAMES.get(metric, metric.upper())


def format_ad_unit_name(ad_unit: str) -> str:
    """Format ad unit name for display.

    Converts internal snake_case identifiers to human-readable format.

    Args:
        ad_unit: Internal ad unit identifier (e.g., "Taylor_Swift").

    Returns:
        Human-readable name (e.g., "Taylor Swift").
    """
    return ad_unit.replace("_", " ")


def get_model_color(model: str) -> str:
    """Get color for a model with fallback to default.

    Args:
        model: Internal model identifier.

    Returns:
        Hex color string for the model.
    """
    return MODEL_COLORS.get(model, DEFAULT_CHART_COLOR)


def get_component_color(component: str) -> str:
    """Get color for a decomposition component with fallback to default.

    Args:
        component: Internal component identifier.

    Returns:
        Hex color string for the component.
    """
    return COMPONENT_COLORS.get(component, DEFAULT_CHART_COLOR)


# Default decomposition component visibility
DEFAULT_SHOW_COMPONENTS: dict[str, bool] = {
    "trend": True,
    "seasonal_weekly": True,
    "seasonal_yearly": True,
    "holiday_effect": True,
}


# Pre-training hypothesis validation results (from spot_check_decomposition.py)
# Format: article -> (metric_name, actual_value, hypothesis_threshold, passed)
HYPOTHESIS_VALIDATION: dict[str, dict[str, str | float | bool]] = {
    "Python_(programming_language)": {
        "metric": "Weekend effect",
        "actual": "-28% to -33%",
        "hypothesis": "> -20%",
        "passed": True,
        "interpretation": "Developers work Monday-Friday; weekend traffic drops as expected.",
    },
    "NFL": {
        "metric": "Weekly amplitude",
        "actual": "25-32%",
        "hypothesis": "> 20%",
        "passed": True,
        "interpretation": "Game-day spikes (Sunday/Monday) create strong weekly patterns.",
    },
    "Bitcoin": {
        "metric": "Weekend effect",
        "actual": "-11% to -12%",
        "hypothesis": "> -15%",
        "passed": True,
        "interpretation": "Crypto is nearly 24/7, but weekday trading still dominates.",
    },
    "Influenza": {
        "metric": "Yearly amplitude",
        "actual": "> 30%",
        "hypothesis": "> 30%",
        "passed": True,
        "interpretation": "October-March flu season creates strong annual cycle.",
    },
    "Taylor_Swift": {
        "metric": "Event-driven",
        "actual": "-16% to +27%",
        "hypothesis": "High volatility",
        "passed": True,
        "interpretation": "Tour/album events drive unpredictable spikes.",
    },
}

# Articles with step changes detected (from README Model Analysis: 20/34 = 59%)
ARTICLES_WITH_STEP_CHANGES: frozenset[str] = frozenset({
    "Barbie_(film)",
    "Oppenheimer_(film)",
    "ChatGPT",
    "Taylor_Swift",
    "Super_Bowl",
    "Bitcoin",
    "Ozempic",
    "NFL",
    "IPhone",
    "Apple_Inc.",
    "Tesla,_Inc.",
    "Google",
    "Microsoft",
    "Netflix",
    "YouTube",
    "Beyonce",
    "Spotify",
    "Stock_market",
    "Amazon_(company)",
    "LeBron_James",
})

# Step change statistics
STEP_CHANGE_STATS = {
    "count": 20,
    "total": 34,
    "percentage": 59,
}
