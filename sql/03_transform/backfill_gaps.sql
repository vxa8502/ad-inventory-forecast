-- Backfill missing dates with interpolated values.
-- Wikipedia public dataset occasionally has data gaps (e.g., 2024-02-18).
-- ARIMA_PLUS requires contiguous time series, so we interpolate missing dates
-- using the average of the surrounding days.
--
-- Uses MERGE for idempotent upsert behavior.

MERGE INTO `{project_id}.{dataset}.daily_impressions` AS target
USING (
    WITH date_spine AS (
        SELECT date
        FROM UNNEST(GENERATE_DATE_ARRAY('{date_start}', '{date_end}')) AS date
    ),
    all_ad_units AS (
        SELECT DISTINCT ad_unit
        FROM `{project_id}.{dataset}.daily_impressions`
    ),
    -- Create full grid of expected (date, ad_unit) combinations
    full_grid AS (
        SELECT d.date, a.ad_unit
        FROM date_spine d
        CROSS JOIN all_ad_units a
    ),
    -- Join with existing data, marking missing rows
    with_existing AS (
        SELECT
            g.date,
            g.ad_unit,
            e.daily_impressions,
            e.desktop_impressions,
            e.mobile_impressions,
            e.date IS NULL AS is_missing
        FROM full_grid g
        LEFT JOIN `{project_id}.{dataset}.daily_impressions` e
            ON g.date = e.date AND g.ad_unit = e.ad_unit
    ),
    -- Use window functions to get prev/next non-null values for interpolation
    with_neighbors AS (
        SELECT
            date,
            ad_unit,
            is_missing,
            -- For missing rows, get the last known value before this date
            LAST_VALUE(daily_impressions IGNORE NULLS) OVER (
                PARTITION BY ad_unit ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ) AS prev_daily,
            LAST_VALUE(desktop_impressions IGNORE NULLS) OVER (
                PARTITION BY ad_unit ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ) AS prev_desktop,
            LAST_VALUE(mobile_impressions IGNORE NULLS) OVER (
                PARTITION BY ad_unit ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ) AS prev_mobile,
            -- Get the next known value after this date
            FIRST_VALUE(daily_impressions IGNORE NULLS) OVER (
                PARTITION BY ad_unit ORDER BY date
                ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
            ) AS next_daily,
            FIRST_VALUE(desktop_impressions IGNORE NULLS) OVER (
                PARTITION BY ad_unit ORDER BY date
                ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
            ) AS next_desktop,
            FIRST_VALUE(mobile_impressions IGNORE NULLS) OVER (
                PARTITION BY ad_unit ORDER BY date
                ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
            ) AS next_mobile
        FROM with_existing
    ),
    -- Calculate interpolated values for missing dates
    interpolated AS (
        SELECT
            date,
            ad_unit,
            CAST(ROUND((prev_daily + next_daily) / 2.0) AS INT64) AS daily_impressions,
            CAST(ROUND((prev_desktop + next_desktop) / 2.0) AS INT64) AS desktop_impressions,
            CAST(ROUND((prev_mobile + next_mobile) / 2.0) AS INT64) AS mobile_impressions,
            EXTRACT(DAYOFWEEK FROM date) AS day_of_week,
            EXTRACT(DAYOFWEEK FROM date) IN (1, 7) AS is_weekend,
            EXTRACT(QUARTER FROM date) AS quarter,
            EXTRACT(WEEK FROM date) AS week_of_year
        FROM with_neighbors
        WHERE is_missing
            AND prev_daily IS NOT NULL
            AND next_daily IS NOT NULL
    ),
    -- Add holiday info
    with_holidays AS (
        SELECT
            i.*,
            h.holiday_date IS NOT NULL AS is_holiday,
            h.holiday_name,
            (
                SELECT MIN(DATE_DIFF(h2.holiday_date, i.date, DAY))
                FROM `{project_id}.{dataset}.us_holidays` h2
                WHERE h2.holiday_date >= i.date
            ) AS days_to_next_holiday
        FROM interpolated i
        LEFT JOIN `{project_id}.{dataset}.us_holidays` h
            ON i.date = h.holiday_date
    )
    SELECT * FROM with_holidays
) AS source
ON target.date = source.date AND target.ad_unit = source.ad_unit
WHEN NOT MATCHED THEN
    INSERT (
        date, ad_unit, daily_impressions, desktop_impressions, mobile_impressions,
        day_of_week, is_weekend, quarter, week_of_year,
        is_holiday, holiday_name, days_to_next_holiday
    )
    VALUES (
        source.date, source.ad_unit, source.daily_impressions,
        source.desktop_impressions, source.mobile_impressions,
        source.day_of_week, source.is_weekend, source.quarter, source.week_of_year,
        source.is_holiday, source.holiday_name, source.days_to_next_holiday
    );
