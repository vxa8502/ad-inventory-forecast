"""Extend US holidays CSV with 2025 dates.

Usage:
    python -m scripts.extend_holidays
"""

from __future__ import annotations

import csv
from pathlib import Path

from config import settings

# 2025 US holidays (21 entries)
HOLIDAYS_2025 = [
    ("2025-01-01", "New Year's Day", "true"),
    ("2025-01-20", "Martin Luther King Jr. Day", "false"),
    ("2025-02-09", "Super Bowl Sunday", "true"),
    ("2025-02-14", "Valentine's Day", "false"),
    ("2025-02-17", "Presidents Day", "false"),
    ("2025-04-20", "Easter Sunday", "true"),
    ("2025-05-11", "Mother's Day", "true"),
    ("2025-05-26", "Memorial Day", "true"),
    ("2025-06-15", "Father's Day", "true"),
    ("2025-06-19", "Juneteenth", "false"),
    ("2025-07-04", "Independence Day", "true"),
    ("2025-09-01", "Labor Day", "true"),
    ("2025-10-13", "Columbus Day", "false"),
    ("2025-10-31", "Halloween", "true"),
    ("2025-11-11", "Veterans Day", "false"),
    ("2025-11-27", "Thanksgiving Day", "true"),
    ("2025-11-28", "Black Friday", "true"),
    ("2025-12-01", "Cyber Monday", "true"),
    ("2025-12-24", "Christmas Eve", "true"),
    ("2025-12-25", "Christmas Day", "true"),
    ("2025-12-31", "New Year's Eve", "true"),
]


def extend_holidays() -> int:
    """Append 2025 holidays to us_holidays.csv.

    Returns:
        Number of holidays added.
    """
    csv_path = settings.PROJECT_ROOT / "data" / "reference" / "us_holidays.csv"

    # Read existing holidays to check for duplicates
    existing_dates: set[str] = set()
    with open(csv_path, "r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            existing_dates.add(row["holiday_date"])

    # Filter out any dates that already exist
    new_holidays = [h for h in HOLIDAYS_2025 if h[0] not in existing_dates]

    if not new_holidays:
        print("No new holidays to add (2025 already present)")
        return 0

    # Append new holidays
    with open(csv_path, "a", newline="") as f:
        writer = csv.writer(f)
        for holiday in new_holidays:
            writer.writerow(holiday)

    print(f"Added {len(new_holidays)} holidays for 2025")
    return len(new_holidays)


def main() -> None:
    """CLI entrypoint."""
    count = extend_holidays()
    if count > 0:
        csv_path = settings.PROJECT_ROOT / "data" / "reference" / "us_holidays.csv"
        print(f"Updated: {csv_path}")
        print("Re-run pipeline to upload to BigQuery")


if __name__ == "__main__":
    main()
