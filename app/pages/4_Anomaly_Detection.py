"""Anomaly Detection - Flagged outliers with event cross-reference."""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from config.events import KNOWN_EVENTS, KNOWN_EVENT_KEYS, EVENT_KEY_SEPARATOR

from app.components.charts import anomaly_chart
from app.components.filters import anomaly_detection_filters
from app.components.tables import anomaly_events_table
from app.constants import ValidationMessages
from app.data.queries import get_all_anomalies_for_ad_unit, get_anomalies
from app.utils.helpers import display_metric_columns, init_page, require_dataframe, to_iso_date_str
from app.utils.validation import require_selections
from app.messages import (
    ANOMALY_DETECTION_ABOUT,
    ANOMALY_DETECTION_INTRO,
    XREG_FAILURE_CALLOUT,
    XREG_FAILURE_AD_UNITS,
)

init_page("Anomaly Detection", ANOMALY_DETECTION_INTRO)

# Sidebar filters
selected_ad_units, selected_fold, threshold = anomaly_detection_filters()

# Validation
require_selections(ad_units=selected_ad_units) or st.stop()

# XREG failure callout for Barbenheimer articles
if any(ad_unit in XREG_FAILURE_AD_UNITS for ad_unit in selected_ad_units):
    st.warning(XREG_FAILURE_CALLOUT)

# Display chart for each ad unit (spinner handled by @cached_query decorator)
for ad_unit in selected_ad_units:
    # Get all data points for chart
    all_data: pd.DataFrame = get_all_anomalies_for_ad_unit(ad_unit, selected_fold)

    if not require_dataframe(all_data, ValidationMessages.NO_ANOMALY_DATA.format(ad_unit=ad_unit)):
        continue

    fig: go.Figure = anomaly_chart(all_data, ad_unit, threshold)
    st.plotly_chart(fig, width="stretch")

st.divider()

# Anomaly table with event cross-reference
st.subheader("Detected Anomalies")

anomalies_df: pd.DataFrame = get_anomalies(selected_ad_units, selected_fold, threshold)

if require_dataframe(anomalies_df, ValidationMessages.NO_ANOMALIES_ABOVE_THRESHOLD):
    # Vectorized matching against pre-built event keys (O(1) lookup)
    anomaly_keys: pd.Series = anomalies_df["ad_unit"] + EVENT_KEY_SEPARATOR + anomalies_df["date"].apply(to_iso_date_str)
    matched_events: int = anomaly_keys.isin(KNOWN_EVENT_KEYS).sum()
    avg_prob: float = anomalies_df["anomaly_probability"].mean()

    display_metric_columns({
        "Total Anomalies": str(len(anomalies_df)),
        "Matched to Known Events": str(matched_events),
        "Avg Probability": f"{avg_prob:.1%}",
    })

    st.divider()

    anomaly_events_table(anomalies_df, KNOWN_EVENTS)

st.divider()

st.markdown(ANOMALY_DETECTION_ABOUT)
