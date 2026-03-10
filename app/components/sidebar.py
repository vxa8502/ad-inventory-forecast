"""Sidebar rendering utilities."""

from __future__ import annotations

__all__ = [
    "render_author_section",
    "sidebar_filters",
]

from contextlib import contextmanager
from typing import Generator

import streamlit as st

from app.components.filters import refresh_button
from app.messages import AUTHOR_NAME, GITHUB_URL, PROJECT_TAGLINE


def render_author_section() -> None:
    """Render the author info section at bottom of sidebar."""
    st.divider()
    st.markdown(f"**{AUTHOR_NAME}**")
    st.caption(PROJECT_TAGLINE)
    st.markdown(f"[View on GitHub]({GITHUB_URL})")


@contextmanager
def sidebar_filters(header: str = "Filters") -> Generator[None, None, None]:
    """Context manager for sidebar with header and refresh button.

    Wraps filter widgets in a consistent sidebar structure:
    - Header at top
    - Yield for filter content
    - Refresh button at bottom
    - Author section at very bottom

    Args:
        header: Sidebar header text.

    Yields:
        Control to caller for filter widget rendering.

    Example:
        with sidebar_filters():
            selected = ad_unit_selector(...)
            st.divider()
            fold = fold_selector(...)
            st.divider()
    """
    with st.sidebar:
        st.header(header)
        yield
        refresh_button()
        render_author_section()
