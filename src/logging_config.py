"""Centralized logging configuration."""

import logging
import sys

__all__ = ["setup_logging"]

_state = {"configured": False}


def setup_logging(level: int = logging.INFO, force: bool = False) -> None:
    """Configure logging with consistent format across all modules.

    Args:
        level: Logging level (default INFO).
        force: If True, reconfigure even if already set up.
    """
    if _state["configured"] and not force:
        return

    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        stream=sys.stderr,
        force=force,
    )
    _state["configured"] = True
