"""Centralized logging configuration."""

from __future__ import annotations

import logging
import sys
from functools import lru_cache

__all__ = ["setup_logging"]


@lru_cache(maxsize=1)
def _configure_logging(level: int) -> bool:
    """Internal: configure logging once per level.

    Args:
        level: Logging level.

    Returns:
        True after configuration.
    """
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        stream=sys.stderr,
        force=True,
    )
    return True


def setup_logging(level: int = logging.INFO, force: bool = False) -> None:
    """Configure logging with consistent format across all modules.

    Args:
        level: Logging level (default INFO).
        force: If True, clear cache and reconfigure.
    """
    if force:
        _configure_logging.cache_clear()
    _configure_logging(level)


