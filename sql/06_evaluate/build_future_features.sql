-- Build future calendar features for XREG forecasts
-- Generates features for the test period (forecast horizon)
-- Required for ML.FORECAST with ARIMA_PLUS_XREG models
--
-- Important: Creates one row per (date, ad_unit) combination because
-- ARIMA_PLUS_XREG trained with time_series_id_col requires regressors
-- to include the series ID column for proper matching at forecast time.
--
-- Timezone: All date calculations use UTC (BigQuery default).
-- Wikipedia pageviews are UTC-aligned. Adapt if using local-timezone data.
--
-- Atomicity: DELETE then INSERT is not atomic in BigQuery.
-- If INSERT fails after DELETE, data is lost. Re-run to recover.

-- Clear existing features for this date range
DELETE FROM `{project_id}.{dataset}.future_features`
WHERE date BETWEEN DATE '{test_start}' AND DATE '{test_end}';

-- Generate calendar features for forecast horizon (one row per date x ad_unit)
INSERT INTO `{project_id}.{dataset}.future_features` (
    date, ad_unit, day_of_week, is_weekend, quarter, week_of_year,
    is_holiday, holiday_name, days_to_next_holiday
)
WITH date_spine AS (
    SELECT date
    FROM UNNEST(
        GENERATE_DATE_ARRAY(DATE '{test_start}', DATE '{test_end}')
    ) AS date
),
ad_units AS (
    SELECT DISTINCT ad_unit
    FROM `{project_id}.{dataset}.daily_impressions`
),
date_ad_unit_cross AS (
    SELECT d.date, a.ad_unit
    FROM date_spine d
    CROSS JOIN ad_units a
),
holidays AS (
    SELECT holiday_date, holiday_name
    FROM `{project_id}.{dataset}.us_holidays`
),
next_holiday AS (
    SELECT
        d.date,
        MIN(h.holiday_date) AS next_holiday_date
    FROM date_spine d
    LEFT JOIN holidays h ON h.holiday_date >= d.date
    GROUP BY d.date
)
SELECT
    dau.date,
    dau.ad_unit,
    EXTRACT(DAYOFWEEK FROM dau.date) AS day_of_week,
    EXTRACT(DAYOFWEEK FROM dau.date) IN (1, 7) AS is_weekend,
    EXTRACT(QUARTER FROM dau.date) AS quarter,
    EXTRACT(WEEK FROM dau.date) AS week_of_year,
    h.holiday_date IS NOT NULL AS is_holiday,
    h.holiday_name,
    DATE_DIFF(nh.next_holiday_date, dau.date, DAY) AS days_to_next_holiday
FROM date_ad_unit_cross dau
LEFT JOIN holidays h ON dau.date = h.holiday_date
LEFT JOIN next_holiday nh ON dau.date = nh.date;
