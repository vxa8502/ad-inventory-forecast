"""Session state management for the Streamlit dashboard."""

from __future__ import annotations

import streamlit as st
from google.cloud import bigquery

from config import settings
from app.constants import (
    DEFAULT_FILTER_AD_UNITS,
    DEFAULT_ANOMALY_THRESHOLD,
    FOLD_CROSS_AVG,
)


_SESSION_INITIALIZED_KEY = "_app_session_initialized"


def init_session_state() -> None:
    """Initialize session state with default values (idempotent).

    Sets up default selections for filters and caches that persist
    across page navigation. Safe to call multiple times; subsequent
    calls return immediately after first initialization.
    """
    if _SESSION_INITIALIZED_KEY in st.session_state:
        return

    # Validate defaults against available articles
    valid_ad_units = [a for a in DEFAULT_FILTER_AD_UNITS if a in settings.ARTICLES]
    if not valid_ad_units:
        valid_ad_units = settings.ARTICLES[:3]

    defaults = {
        "selected_ad_units": valid_ad_units,
        "selected_models": settings.MODEL_NAMES.copy(),
        "selected_fold": FOLD_CROSS_AVG,
        "show_ci": True,
        "anomaly_threshold": DEFAULT_ANOMALY_THRESHOLD,
    }

    for key, value in defaults.items():
        st.session_state.setdefault(key, value)

    st.session_state[_SESSION_INITIALIZED_KEY] = True


@st.cache_resource
def get_bq_client() -> bigquery.Client:
    """Get cached BigQuery client.

    Returns:
        Configured BigQuery client instance.
    """
    return bigquery.Client(
        project=settings.PROJECT_ID,
        location=settings.LOCATION,
    )


def clear_query_cache() -> None:
    """Clear all cached query results.

    Useful when user wants to refresh data from BigQuery.
    """
    st.cache_data.clear()
