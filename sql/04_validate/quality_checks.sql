-- Quality Check 1: Row count per ad_unit matches expected date range
WITH expected_days AS (
    SELECT DATE_DIFF(DATE '{date_end}', DATE '{date_start}', DAY) + 1 AS expected_count
),
actual_counts AS (
    SELECT
        ad_unit,
        COUNT(*) AS actual_count
    FROM `{project_id}.{dataset}.daily_impressions`
    GROUP BY 1
)
SELECT
    'row_count_check' AS check_name,
    ac.ad_unit,
    ac.actual_count,
    ed.expected_count,
    CASE WHEN ac.actual_count = ed.expected_count THEN 'PASS' ELSE 'FAIL' END AS status
FROM actual_counts ac
CROSS JOIN expected_days ed;

-- Quality Check 2: No NULL impressions
SELECT
    'null_impressions_check' AS check_name,
    ad_unit,
    COUNT(*) AS null_count,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM `{project_id}.{dataset}.daily_impressions`
WHERE daily_impressions IS NULL
GROUP BY ad_unit;

-- Quality Check 3: No negative values
SELECT
    'negative_values_check' AS check_name,
    ad_unit,
    COUNT(*) AS negative_count,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM `{project_id}.{dataset}.daily_impressions`
WHERE daily_impressions < 0 OR desktop_impressions < 0 OR mobile_impressions < 0
GROUP BY ad_unit;

-- Quality Check 4: Date continuity (no gaps)
WITH date_gaps AS (
    SELECT
        ad_unit,
        date,
        LAG(date) OVER (PARTITION BY ad_unit ORDER BY date) AS prev_date,
        DATE_DIFF(date, LAG(date) OVER (PARTITION BY ad_unit ORDER BY date), DAY) AS day_diff
    FROM `{project_id}.{dataset}.daily_impressions`
)
SELECT
    'date_continuity_check' AS check_name,
    ad_unit,
    COUNT(*) AS gap_count,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM date_gaps
WHERE day_diff > 1
GROUP BY ad_unit;

-- Quality Check 5: Holiday join success rate
SELECT
    'holiday_join_check' AS check_name,
    COUNTIF(is_holiday) AS holiday_matches,
    COUNT(*) AS total_rows,
    ROUND(COUNTIF(is_holiday) / COUNT(*) * 100, 2) AS match_rate_pct,
    CASE WHEN COUNTIF(is_holiday) > 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM `{project_id}.{dataset}.daily_impressions`;
