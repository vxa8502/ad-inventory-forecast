"""Decomposition - ARIMA component breakdown visualization."""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from app.components.charts import decomposition_chart
from app.components.filters import decomposition_filters
from app.constants import (
    ARTICLES_WITH_STEP_CHANGES,
    HYPOTHESIS_VALIDATION,
    STEP_CHANGE_STATS,
    ValidationMessages,
)
from app.data.queries import (
    get_decomposition,
    get_holiday_effects,
    get_holidays_in_range,
)
from app.utils.helpers import (
    display_component_stats,
    init_page,
    require_dataframe,
    series_max_abs,
    series_range,
    to_iso_date_str,
)
from app.messages import (
    DECOMPOSITION_ABOUT,
    DECOMPOSITION_INTRO,
    DECOMPOSITION_INTERPRETABILITY_NOTE,
    HYPOTHESIS_CALLOUT_TEMPLATE,
    STEP_CHANGE_CALLOUT,
)

init_page("ARIMA Decomposition", DECOMPOSITION_INTRO)

# Interpretability note (black-box vs explainable tradeoff)
st.info(DECOMPOSITION_INTERPRETABILITY_NOTE)

# Sidebar filters
selected_ad_unit, selected_fold, show_components = decomposition_filters()

# Load decomposition data (spinner handled by @cached_query decorator)
decomp_df: pd.DataFrame = get_decomposition(selected_ad_unit, selected_fold)

if not require_dataframe(
    decomp_df,
    ValidationMessages.NO_DECOMPOSITION_DATA.format(ad_unit=selected_ad_unit, fold=selected_fold),
):
    st.stop()

# Hypothesis validation callout (if article has pre-training hypothesis)
if selected_ad_unit in HYPOTHESIS_VALIDATION:
    hypothesis = HYPOTHESIS_VALIDATION[selected_ad_unit]
    status_icon = "PASSED" if hypothesis["passed"] else "REVIEW"
    st.success(
        HYPOTHESIS_CALLOUT_TEMPLATE.format(
            metric=hypothesis["metric"],
            actual=hypothesis["actual"],
            hypothesis=hypothesis["hypothesis"],
            status=status_icon,
            interpretation=hypothesis["interpretation"],
        )
    )

# Step change callout (if article has step changes)
if selected_ad_unit in ARTICLES_WITH_STEP_CHANGES:
    st.warning(
        STEP_CHANGE_CALLOUT.format(
            count=STEP_CHANGE_STATS["count"],
            total=STEP_CHANGE_STATS["total"],
            pct=STEP_CHANGE_STATS["percentage"],
        )
    )

# Load holiday data for annotations
date_min = to_iso_date_str(decomp_df["date"].min())
date_max = to_iso_date_str(decomp_df["date"].max())
holidays_df: pd.DataFrame = get_holidays_in_range(date_min, date_max)
holiday_effects_df: pd.DataFrame = get_holiday_effects(selected_ad_unit, selected_fold)

# Display chart with holiday annotations
fig: go.Figure = decomposition_chart(
    decomp_df,
    selected_ad_unit,
    show_components,
    holidays_df=holidays_df,
    holiday_effects_df=holiday_effects_df,
)
st.plotly_chart(fig, width="stretch")

st.divider()

# Component statistics
st.subheader("Component Statistics")

display_component_stats(decomp_df, [
    ("trend", "Trend Range", series_range),
    ("seasonal_weekly", "Weekly Amplitude", series_range),
    ("seasonal_yearly", "Yearly Amplitude", series_range),
    ("holiday_effect", "Max Holiday Effect", series_max_abs),
])

st.divider()

st.markdown(DECOMPOSITION_ABOUT)
