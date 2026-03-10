"""Shared validation helpers for the dashboard."""

from __future__ import annotations

from typing import Sequence

import streamlit as st

from app.constants import ValidationMessages


def require_selection(
    items: Sequence[str],
    message: str = ValidationMessages.SELECT_AD_UNIT,
) -> bool:
    """Validate that at least one item is selected, display warning if not.

    Centralizes the repeated pattern of checking for empty selections
    and displaying a warning message. Use with st.stop() for early exit.

    Args:
        items: Sequence of selected items to validate.
        message: Warning message to display if selection is empty.

    Returns:
        True if items has at least one selection, False if empty.

    Example:
        >>> if not require_selection(selected_ad_units, ValidationMessages.SELECT_AD_UNIT):
        ...     st.stop()
        >>> # Continue with selected_ad_units...

    Compact pattern (recommended):
        >>> require_selection(selected_ad_units) or st.stop()
    """
    if not items:
        st.warning(message)
        return False
    return True


def require_ad_unit_selection(ad_units: Sequence[str]) -> bool:
    """Validate ad unit selection with standard message.

    Convenience wrapper around require_selection for the common case.

    Args:
        ad_units: List of selected ad unit names.

    Returns:
        True if at least one ad unit selected, False if empty.

    Example:
        >>> require_ad_unit_selection(selected_ad_units) or st.stop()
    """
    return require_selection(ad_units, ValidationMessages.SELECT_AD_UNIT)


def require_model_selection(models: Sequence[str]) -> bool:
    """Validate model selection with standard message.

    Convenience wrapper around require_selection for the common case.

    Args:
        models: List of selected model names.

    Returns:
        True if at least one model selected, False if empty.

    Example:
        >>> require_model_selection(selected_models) or st.stop()
    """
    return require_selection(models, ValidationMessages.SELECT_MODEL)


def require_selections(**named_selections: Sequence[str]) -> bool:
    """Validate multiple selections at once, show warnings for empty ones.

    Batch validation that consolidates multiple require_*_selection calls
    into a single line. Shows appropriate warning for each empty selection.

    Args:
        **named_selections: Keyword arguments where key is selection type
            (ad_units, models) and value is the sequence to validate.

    Returns:
        True if all selections are non-empty, False if any are empty.

    Example:
        >>> require_selections(
        ...     ad_units=selected_ad_units,
        ...     models=selected_models
        ... ) or st.stop()
    """
    # Map common names to their validation messages
    message_map = {
        "ad_units": ValidationMessages.SELECT_AD_UNIT,
        "models": ValidationMessages.SELECT_MODEL,
    }

    all_valid = True
    for name, items in named_selections.items():
        if not items:
            message = message_map.get(name, f"Please select at least one {name}")
            st.warning(message)
            all_valid = False
    return all_valid
