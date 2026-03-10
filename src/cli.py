"""Common CLI utilities for script entrypoints."""

from __future__ import annotations

import argparse
import logging
import sys

from config import settings
from src.logging_config import setup_logging

__all__ = [
    "add_common_args",
    "add_dry_run_arg",
    "add_verbose_arg",
    "configure_logging_from_args",
    "require_project_id",
]


def add_verbose_arg(parser: argparse.ArgumentParser) -> None:
    """Add --verbose/-v argument to parser."""
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose debug logging",
    )


def add_dry_run_arg(parser: argparse.ArgumentParser) -> None:
    """Add --dry-run argument to parser."""
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Estimate query costs without executing",
    )


def add_common_args(parser: argparse.ArgumentParser) -> None:
    """Add common CLI arguments (--dry-run, --verbose)."""
    add_dry_run_arg(parser)
    add_verbose_arg(parser)


def configure_logging_from_args(args: argparse.Namespace) -> None:
    """Configure logging based on parsed arguments.

    Args:
        args: Parsed arguments with optional 'verbose' attribute.
    """
    if getattr(args, "verbose", False):
        setup_logging(level=logging.DEBUG, force=True)


def require_project_id() -> None:
    """Exit with error if GCP_PROJECT_ID is not configured."""
    if not settings.PROJECT_ID:
        print("Error: GCP_PROJECT_ID environment variable not set.")
        print("Copy .env.example to .env and configure your project ID.")
        sys.exit(1)
