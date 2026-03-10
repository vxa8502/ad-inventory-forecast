"""Sidebar filter widgets for the dashboard."""

from __future__ import annotations

import logging

import streamlit as st

from config import settings
from app.constants import (
    format_ad_unit_name,
    format_model_name,
    FOLD_LABELS,
    FOLD_CROSS_AVG,
    FOLD_1,
    FOLD_2,
    ANOMALY_THRESHOLD_MIN,
    ANOMALY_THRESHOLD_MAX,
    ANOMALY_THRESHOLD_DEFAULT,
    DEFAULT_FILTER_AD_UNITS,
    CONTENT_TYPE_ALL,
    CONTENT_TYPE_STABLE,
    CONTENT_TYPE_EVENT_DRIVEN,
    CONTENT_TYPE_LABELS,
    WidgetKeys,
    WidgetLabels,
)
from app.utils.state import clear_query_cache

logger = logging.getLogger(__name__)


def ad_unit_selector(
    max_items: int = 5,
    key: str = "ad_unit_filter",
    default: list[str] | None = None,
    group_by_vertical: bool = False,
) -> list[str]:
    """Multi-select widget for ad units.

    Args:
        max_items: Maximum number of selections allowed.
        key: Unique key for the widget.
        default: Default selected values.
        group_by_vertical: If True, show grouped expanders by vertical.

    Returns:
        List of selected ad unit names.
    """
    if default is None:
        default = DEFAULT_FILTER_AD_UNITS

    # Ensure defaults exist in options (use set for O(1) membership test)
    articles_set = set(settings.ARTICLES)
    filtered_defaults = [d for d in default if d in articles_set]
    if not filtered_defaults:
        logger.warning(f"No valid defaults in {default}; using first {max_items} articles")
    valid_defaults = filtered_defaults or settings.ARTICLES[:max_items]

    if not group_by_vertical:
        return st.multiselect(
            WidgetLabels.AD_UNITS,
            options=settings.ARTICLES,
            default=valid_defaults[:max_items],
            max_selections=max_items,
            key=key,
            help=f"Select up to {max_items} ad units to compare",
        )

    # Grouped selection by vertical
    st.markdown(f"**{WidgetLabels.AD_UNITS}**")
    st.caption(f"Select up to {max_items} ad units")
    selected: list[str] = []
    default_set = set(valid_defaults[:max_items])

    for vertical, articles in settings.ARTICLE_VERTICALS.items():
        with st.expander(f"{vertical} ({len(articles)})"):
            for article in articles:
                is_default = article in default_set
                disabled = len(selected) >= max_items and not is_default
                if st.checkbox(
                    format_ad_unit_name(article),
                    value=is_default,
                    key=f"{key}_{article}",
                    disabled=disabled,
                ):
                    if article not in selected:
                        selected.append(article)

    return selected


def single_ad_unit_selector(
    key: str = "single_ad_unit",
    default: str | None = None,
) -> str:
    """Single-select widget for ad unit.

    Args:
        key: Unique key for the widget.
        default: Default selected value. Falls back to first available article
            if None or if provided value is not in settings.ARTICLES.

    Returns:
        Selected ad unit name (guaranteed to be valid).
    """
    if default is None or default not in settings.ARTICLES:
        if default is not None:
            logger.warning(f"Invalid default ad_unit '{default}'; using first available")
        default = settings.ARTICLES[0]

    idx = settings.ARTICLES.index(default)
    return st.selectbox(
        WidgetLabels.AD_UNIT,
        options=settings.ARTICLES,
        index=idx,
        key=key,
    )


def model_selector(
    key: str = "model_filter",
    default: list[str] | None = None,
) -> list[str]:
    """Checkbox group for model selection.

    Args:
        key: Unique key prefix for widgets.
        default: Default selected models.

    Returns:
        List of selected model names.
    """
    if default is None:
        default = settings.MODEL_NAMES.copy()

    st.markdown(f"**{WidgetLabels.MODELS}**")

    selected_models: list[str] = []
    for model in settings.MODEL_NAMES:
        if st.checkbox(
            format_model_name(model),
            value=model in default,
            key=f"{key}_{model}",
        ):
            selected_models.append(model)
    return selected_models


def fold_selector(
    key: str = "fold_filter",
    include_cross_fold: bool = True,
) -> str:
    """Radio button selector for fold.

    Args:
        key: Unique key for the widget.
        include_cross_fold: Whether to include cross_fold_avg option.

    Returns:
        Selected fold name (FOLD_CROSS_AVG, FOLD_1, or FOLD_2).
    """
    options = [FOLD_CROSS_AVG, FOLD_1, FOLD_2] if include_cross_fold else [FOLD_1, FOLD_2]

    selected: str = st.radio(
        WidgetLabels.VALIDATION_FOLD,
        options=options,
        format_func=lambda x: FOLD_LABELS.get(x, x),
        key=key,
        horizontal=False,
    )
    return selected


def ci_toggle(key: str = "ci_toggle") -> bool:
    """Toggle for showing confidence intervals.

    Args:
        key: Unique key for the widget.

    Returns:
        Whether to show confidence intervals.
    """
    return st.checkbox(
        WidgetLabels.SHOW_CI,
        value=True,
        key=key,
        help="Display 95% confidence bands around forecasts",
    )


def horizon_selector(key: str = "horizon_selector") -> int:
    """Slider for selecting forecast horizon.

    Args:
        key: Unique key for the widget.

    Returns:
        Selected horizon in days (30, 60, or 90).
    """
    return st.select_slider(
        WidgetLabels.FORECAST_HORIZON,
        options=settings.FORECAST_HORIZON_OPTIONS,
        value=90,
        key=key,
        help="Number of forecast days to display",
    )


def anomaly_threshold_slider(
    key: str = "anomaly_threshold",
) -> float:
    """Slider for anomaly probability threshold.

    Args:
        key: Unique key for the widget.

    Returns:
        Selected threshold value.
    """
    return st.slider(
        WidgetLabels.ANOMALY_THRESHOLD,
        min_value=ANOMALY_THRESHOLD_MIN,
        max_value=ANOMALY_THRESHOLD_MAX,
        value=ANOMALY_THRESHOLD_DEFAULT,
        step=0.01,
        key=key,
        help="Minimum probability to flag as anomaly",
    )


def content_type_selector(key: str = "content_type") -> str:
    """Radio selector for content type filtering.

    Filters ad units by coefficient of variation (CV):
    - Stable: CV < 0.5 (predictable patterns)
    - Event-driven: CV >= 0.5 (volatile, spiky traffic)

    Args:
        key: Unique key for the widget.

    Returns:
        Selected content type (CONTENT_TYPE_ALL, CONTENT_TYPE_STABLE,
        or CONTENT_TYPE_EVENT_DRIVEN).
    """
    options = [CONTENT_TYPE_ALL, CONTENT_TYPE_STABLE, CONTENT_TYPE_EVENT_DRIVEN]

    selected: str = st.radio(
        WidgetLabels.CONTENT_TYPE,
        options=options,
        format_func=lambda x: CONTENT_TYPE_LABELS.get(x, x),
        key=key,
        horizontal=True,
        help="Filter by traffic volatility (CV = coefficient of variation)",
    )
    return selected


def refresh_button() -> None:
    """Button to clear cached data and rerun the app."""
    if st.button(WidgetLabels.REFRESH_DATA, type="secondary", width="stretch"):
        clear_query_cache()
        st.rerun()


# Page-specific filter helpers
# These consolidate repeated sidebar patterns into single function calls


def forecast_explorer_filters() -> tuple[list[str], list[str], str, int, bool]:
    """Build sidebar filters for Forecast Explorer page.

    Returns:
        Tuple of (selected_ad_units, selected_models, selected_fold,
        horizon_days, show_ci).
    """
    from app.components.sidebar import sidebar_filters

    with sidebar_filters():
        selected_ad_units = ad_unit_selector(
            max_items=3,
            key=WidgetKeys.FORECAST_SELECTED_AD_UNITS,
            group_by_vertical=True,
        )
        st.divider()
        selected_models = model_selector(key=WidgetKeys.FORECAST_SELECTED_MODELS)
        st.divider()
        selected_fold = fold_selector(
            key=WidgetKeys.FORECAST_SELECTED_FOLD,
            include_cross_fold=False,
        )
        st.divider()
        horizon_days = horizon_selector(key=WidgetKeys.FORECAST_HORIZON)
        st.divider()
        show_ci = ci_toggle(key=WidgetKeys.FORECAST_SHOW_CI)

    return selected_ad_units, selected_models, selected_fold, horizon_days, show_ci


def model_comparison_filters() -> tuple[str, str]:
    """Build sidebar filters for Model Comparison page.

    Returns:
        Tuple of (selected_fold, content_type).
    """
    from app.components.sidebar import sidebar_filters

    with sidebar_filters():
        selected_fold = fold_selector(
            key=WidgetKeys.COMPARISON_SELECTED_FOLD,
            include_cross_fold=True,
        )
        st.divider()
        content_type = content_type_selector(key=WidgetKeys.COMPARISON_CONTENT_TYPE)
        st.divider()

    return selected_fold, content_type


def decomposition_filters() -> tuple[str, str, dict[str, bool]]:
    """Build sidebar filters for Decomposition page.

    Returns:
        Tuple of (selected_ad_unit, selected_fold, show_components dict).
    """
    from app.components.sidebar import sidebar_filters

    with sidebar_filters():
        selected_ad_unit = single_ad_unit_selector(key=WidgetKeys.DECOMP_SELECTED_AD_UNIT)
        st.divider()
        selected_fold = fold_selector(
            key=WidgetKeys.DECOMP_SELECTED_FOLD,
            include_cross_fold=False,
        )
        st.divider()
        st.subheader(WidgetLabels.COMPONENTS)
        show_components = {
            "trend": st.checkbox(
                WidgetLabels.TREND, value=True, key=WidgetKeys.DECOMP_SHOW_TREND
            ),
            "seasonal_weekly": st.checkbox(
                WidgetLabels.WEEKLY_SEASONALITY, value=True, key=WidgetKeys.DECOMP_SHOW_WEEKLY
            ),
            "seasonal_yearly": st.checkbox(
                WidgetLabels.YEARLY_SEASONALITY, value=True, key=WidgetKeys.DECOMP_SHOW_YEARLY
            ),
            "holiday_effect": st.checkbox(
                WidgetLabels.HOLIDAY_EFFECT, value=True, key=WidgetKeys.DECOMP_SHOW_HOLIDAY
            ),
        }
        st.divider()

    return selected_ad_unit, selected_fold, show_components


def anomaly_detection_filters() -> tuple[list[str], str, float]:
    """Build sidebar filters for Anomaly Detection page.

    Returns:
        Tuple of (selected_ad_units, selected_fold, threshold).
    """
    from app.components.sidebar import sidebar_filters

    with sidebar_filters():
        selected_ad_units = ad_unit_selector(
            max_items=3,
            key=WidgetKeys.ANOMALY_SELECTED_AD_UNITS,
        )
        st.divider()
        selected_fold = fold_selector(
            key=WidgetKeys.ANOMALY_SELECTED_FOLD,
            include_cross_fold=False,
        )
        st.divider()
        threshold = anomaly_threshold_slider(key=WidgetKeys.ANOMALY_THRESHOLD_VALUE)
        st.divider()

    return selected_ad_units, selected_fold, threshold
