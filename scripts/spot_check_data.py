"""Spot-check 10 random rows from the joined training table.

Run this after the pipeline completes to verify:
1. No unexpected nulls from left joins
2. Holiday joins work correctly (is_holiday, days_to_next_holiday)
3. Calendar features are populated correctly

Usage:
    python -m scripts.spot_check_data
"""

import sys

from config import settings
from config.helpers import get_table_id
from src.validators import spot_check_random_rows


def main() -> None:
    """Fetch and display 10 random rows for manual inspection."""
    if not settings.PROJECT_ID:
        print("Error: GCP_PROJECT_ID not set. Copy .env.example to .env")
        sys.exit(1)

    table_id = get_table_id("daily_impressions")

    print("=" * 70)
    print("SPOT CHECK: 10 Random Rows from daily_impressions")
    print("=" * 70)

    rows = spot_check_random_rows(table_id, n=10)

    for i, row in enumerate(rows, 1):
        print(f"\n--- Row {i} ---")
        print(f"  date: {row['date']}")
        print(f"  ad_unit: {row['ad_unit']}")
        print(f"  daily_impressions: {row['daily_impressions']:,}")
        desktop, mobile = row['desktop_impressions'], row['mobile_impressions']
        print(f"  desktop: {desktop:,} | mobile: {mobile:,}")
        print(f"  day_of_week: {row['day_of_week']} | is_weekend: {row['is_weekend']}")
        print(f"  quarter: {row['quarter']} | week_of_year: {row['week_of_year']}")
        holiday_name = row.get('holiday_name', 'NULL')
        print(f"  is_holiday: {row['is_holiday']} | holiday_name: {holiday_name}")
        print(f"  days_to_next_holiday: {row['days_to_next_holiday']}")

    print("\n" + "=" * 70)
    print("VERIFICATION STEPS:")
    print("=" * 70)
    print("[ ] No unexpected NULLs in required columns")
    print("[ ] desktop + mobile = daily_impressions")
    print("[ ] day_of_week in range 1-7 (1=Sunday)")
    print("[ ] is_weekend=TRUE only for day_of_week IN (1, 7)")
    print("[ ] quarter in range 1-4")
    print("[ ] week_of_year in range 1-53")
    print("[ ] is_holiday=TRUE implies days_to_next_holiday=0")
    print("[ ] holiday_name populated when is_holiday=TRUE")
    print("=" * 70)


if __name__ == "__main__":
    main()
