"""Explore Google Trends BQ public dataset for feature engineering assessment."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from google.cloud import bigquery

from config import settings
from src import bq_client


def explore_schema() -> None:
    """Print table schema for Google Trends datasets."""
    client = bq_client.get_client()

    tables_to_check = [
        "bigquery-public-data.google_trends.international_top_terms",
        "bigquery-public-data.google_trends.international_top_rising_terms",
        "bigquery-public-data.google_trends.top_terms",
        "bigquery-public-data.google_trends.top_rising_terms",
    ]

    print("=" * 60)
    print("GOOGLE TRENDS BQ PUBLIC DATASET EXPLORATION")
    print("=" * 60)

    for table_id in tables_to_check:
        print(f"\n>>> {table_id}")
        try:
            table = client.get_table(table_id)
            print(f"    Rows: {table.num_rows:,}")
            print(f"    Size: {table.num_bytes / 1024**3:.2f} GB")
            print("    Schema:")
            for field in table.schema:
                print(f"      - {field.name}: {field.field_type}")
        except Exception as e:
            print(f"    ERROR: {e}")


def sample_international_top_terms() -> None:
    """Sample rows from international_top_terms to understand structure."""
    query = """
    SELECT *
    FROM `bigquery-public-data.google_trends.international_top_terms`
    WHERE country_code = 'US'
    LIMIT 10
    """
    print("\n" + "=" * 60)
    print("SAMPLE: international_top_terms (US)")
    print("=" * 60)

    result = bq_client.run_query(query)
    for row in result:
        print(dict(row))


def check_date_range() -> None:
    """Check available date range in the dataset."""
    query = """
    SELECT
        MIN(week) AS min_week,
        MAX(week) AS max_week,
        COUNT(DISTINCT week) AS weeks_count
    FROM `bigquery-public-data.google_trends.international_top_terms`
    WHERE country_code = 'US'
    """
    print("\n" + "=" * 60)
    print("DATE RANGE: international_top_terms (US)")
    print("=" * 60)

    result = bq_client.run_query(query)
    for row in result:
        print(f"Min week: {row['min_week']}")
        print(f"Max week: {row['max_week']}")
        print(f"Total weeks: {row['weeks_count']}")


def check_article_matches() -> None:
    """Check how many of our target articles appear in Google Trends."""
    articles_sql = ", ".join(f"'{a}'" for a in settings.ARTICLES)

    query = f"""
    WITH our_articles AS (
        SELECT article FROM UNNEST([{articles_sql}]) AS article
    ),
    trends_terms AS (
        SELECT DISTINCT term
        FROM `bigquery-public-data.google_trends.international_top_terms`
        WHERE country_code = 'US'
          AND week BETWEEN '{settings.DATE_START}' AND '{settings.DATE_END}'
    )
    SELECT
        a.article,
        t.term IS NOT NULL AS found_in_trends
    FROM our_articles a
    LEFT JOIN trends_terms t
        ON LOWER(a.article) = LOWER(t.term)
        OR LOWER(REPLACE(a.article, '_', ' ')) = LOWER(t.term)
    ORDER BY found_in_trends DESC, a.article
    """
    print("\n" + "=" * 60)
    print("ARTICLE MATCH CHECK: Our 35 articles vs Google Trends")
    print("=" * 60)

    result = bq_client.run_query(query)
    found = 0
    missing = 0
    for row in result:
        status = "FOUND" if row["found_in_trends"] else "MISSING"
        if row["found_in_trends"]:
            found += 1
        else:
            missing += 1
        print(f"  [{status}] {row['article']}")

    print(f"\nSummary: {found}/{found + missing} articles found in Google Trends")


def check_coverage_for_sample_articles() -> None:
    """Check weekly coverage for a few sample articles."""
    sample_articles = ["Taylor Swift", "Bitcoin", "ChatGPT", "Super Bowl", "NFL"]
    articles_sql = ", ".join(f"'{a}'" for a in sample_articles)

    query = f"""
    SELECT
        term,
        COUNT(DISTINCT week) AS weeks_with_data,
        MIN(week) AS first_week,
        MAX(week) AS last_week,
        AVG(score) AS avg_score
    FROM `bigquery-public-data.google_trends.international_top_terms`
    WHERE country_code = 'US'
      AND week BETWEEN '{settings.DATE_START}' AND '{settings.DATE_END}'
      AND term IN ({articles_sql})
    GROUP BY term
    ORDER BY weeks_with_data DESC
    """
    print("\n" + "=" * 60)
    print("COVERAGE CHECK: Sample articles")
    print("=" * 60)

    result = bq_client.run_query(query)
    rows = list(result)
    if not rows:
        print("  No matches found for sample articles")
    else:
        for row in rows:
            print(f"  {row['term']}: {row['weeks_with_data']} weeks, "
                  f"avg score={row['avg_score']:.1f}")


def main() -> None:
    """Run all exploration queries."""
    if not settings.PROJECT_ID:
        print("Error: GCP_PROJECT_ID not set. Copy .env.example to .env")
        sys.exit(1)

    explore_schema()
    sample_international_top_terms()
    check_date_range()
    check_article_matches()
    check_coverage_for_sample_articles()

    print("\n" + "=" * 60)
    print("EXPLORATION COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
