"""Ad Inventory Forecasting Dashboard - Entry Point."""

from __future__ import annotations

import sys
from pathlib import Path

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st

from app.utils.state import init_session_state
from app.data.queries import get_model_comparison
from app.components.tables import summary_metrics_card
from app.constants import FOLD_CROSS_AVG, METRIC_MAPE
from app.components.sidebar import render_author_section
from app.messages import (
    DASHBOARD_INTRO,
    KEY_FINDING,
    NAVIGATION_GUIDE,
    DATA_COVERAGE,
)

st.set_page_config(
    page_title="Ad Inventory Forecast",
    page_icon="chart_with_upwards_trend",
    layout="wide",
    initial_sidebar_state="expanded",
)

init_session_state()

st.title("Ad Inventory Forecasting Dashboard")

st.markdown(DASHBOARD_INTRO)

st.divider()

st.subheader("Headline Metrics")
st.caption("Cross-fold average of Fold 1 (Jul-Sep 2024) and Fold 2 (Oct-Dec 2024)")

metrics_df = get_model_comparison(FOLD_CROSS_AVG)
summary_metrics_card(metrics_df, METRIC_MAPE)

st.divider()

col1, col2 = st.columns(2)

with col1:
    st.markdown(KEY_FINDING)

with col2:
    st.markdown(NAVIGATION_GUIDE)

st.divider()

st.markdown(DATA_COVERAGE)

# Sidebar author section (main page has no filters, so render directly)
with st.sidebar:
    render_author_section()
