"""Forecast Explorer - Compare model predictions against actuals."""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from config import settings
from config.events import KNOWN_EVENTS

from app.components.charts import forecast_chart, ForecastChartOptions
from app.components.filters import forecast_explorer_filters
from app.constants import HIGHLIGHT_BEST_COLOR, ValidationMessages, format_ad_unit_name
from app.data.queries import (
    get_actuals,
    get_anomalies_for_chart,
    get_forecasts,
    get_metrics_detail,
    get_volatility_metrics,
)
from app.utils.helpers import build_lookup_dict, filter_mape_metrics, format_pivot_labels, init_page, require_dataframe
from app.utils.validation import require_selections
from app.messages import FORECAST_EXPLORER_INTRO

init_page("Forecast Explorer", FORECAST_EXPLORER_INTRO)

# Sidebar filters
selected_ad_units, selected_models, selected_fold, horizon_days, show_ci = forecast_explorer_filters()

# Validation
require_selections(ad_units=selected_ad_units, models=selected_models) or st.stop()

# Get fold config for date range
fold_config: dict = settings.FOLD_CONFIGS_BY_NAME.get(
    selected_fold, settings.FOLD_CONFIGS[0]
).copy()

# Load data (spinner handled by @cached_query decorator)
actuals_df: pd.DataFrame = get_actuals(
    selected_ad_units,
    start_date=fold_config["train_start"],
    end_date=fold_config["test_end"],
)

forecasts_df: pd.DataFrame = get_forecasts(
    selected_ad_units,
    selected_models,
    selected_fold,
)

# Filter forecasts to selected horizon
if not forecasts_df.empty:
    forecasts_df = forecasts_df.sort_values("date")
    min_date = forecasts_df["date"].min()
    max_horizon_date = min_date + pd.Timedelta(days=horizon_days)
    forecasts_df = forecasts_df[forecasts_df["date"] <= max_horizon_date]

# Load volatility metrics for routing recommendation
volatility_df: pd.DataFrame = get_volatility_metrics()
volatility_lookup: dict[str, float] = build_lookup_dict(volatility_df, "ad_unit", "cv")

# Display charts for each ad unit
for ad_unit in selected_ad_units:
    # Check volatility and show warning if high
    cv = volatility_lookup.get(ad_unit, 0.0)
    if cv > settings.VOLATILITY_CV_THRESHOLD:
        st.warning(
            f"High-volatility content (CV={cv:.2f}) - TimesFM recommended for {format_ad_unit_name(ad_unit)}"
        )

    # Get anomalies for this ad unit
    anomalies_df: pd.DataFrame = get_anomalies_for_chart(ad_unit, selected_fold)

    # Get known events for annotation
    known_events = KNOWN_EVENTS.get(ad_unit, [])

    chart_options = ForecastChartOptions(
        show_ci=show_ci,
        anomalies_df=anomalies_df,
        known_events=known_events,
    )
    fig: go.Figure = forecast_chart(actuals_df, forecasts_df, ad_unit, chart_options)
    st.plotly_chart(fig, width="stretch")

st.divider()

# Summary metrics table
st.subheader("Metrics for Selected Ad Units")

metrics_df: pd.DataFrame = get_metrics_detail(selected_fold)

if not require_dataframe(metrics_df, ValidationMessages.NO_METRICS_FOR_FOLD):
    st.stop()

# Filter to selected ad units and models
filtered_metrics: pd.DataFrame = metrics_df[
    (metrics_df["ad_unit"].isin(selected_ad_units)) &
    (metrics_df["model_name"].isin(selected_models))
].copy()

if not require_dataframe(filtered_metrics, ValidationMessages.NO_METRICS_FOR_FILTERS):
    st.stop()

# Pivot for display: rows = ad_unit, columns = model_name, values = MAPE
mape_only: pd.DataFrame = filter_mape_metrics(filtered_metrics)

if not require_dataframe(mape_only, ValidationMessages.NO_MAPE_METRICS):
    st.stop()

pivot: pd.DataFrame = mape_only.pivot(
    index="ad_unit",
    columns="model_name",
    values="metric_value",
)
pivot = format_pivot_labels(pivot)

st.dataframe(
    pivot.style.format("{:.1f}%").highlight_min(axis=1, color=HIGHLIGHT_BEST_COLOR),
    width="stretch",
)
