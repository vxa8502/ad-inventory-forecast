-- Build feature-enriched daily impressions table for forecasting models.
-- Features include:
--   - Calendar: day_of_week (1-7, where 1=Sunday), is_weekend, quarter, week_of_year
--   - Holiday: is_holiday flag, holiday_name, days_to_next_holiday
--
-- Note: Google Trends external regressor was evaluated but scoped out.
-- The BQ public dataset captures trending breakout events, not persistent topic
-- interest—only 1/35 of our Wikipedia articles appeared in US top terms.
-- Calendar features provide cleaner, deterministic cyclical signals.
--
-- Edge case (days_to_next_holiday):
--   When is_holiday=TRUE, days_to_next_holiday=0 (today IS the holiday).
--   This captures pre-holiday ramp-up patterns while being unambiguous.

-- Clear existing data for idempotent re-runs
DELETE FROM `{project_id}.{dataset}.daily_impressions` WHERE TRUE;

INSERT INTO `{project_id}.{dataset}.daily_impressions` (
    date,
    ad_unit,
    daily_impressions,
    desktop_impressions,
    mobile_impressions,
    day_of_week,
    is_weekend,
    quarter,
    week_of_year,
    is_holiday,
    holiday_name,
    days_to_next_holiday
)
WITH pageviews_combined AS (
    SELECT
        date,
        ad_unit,
        SUM(daily_impressions) AS daily_impressions,
        SUM(desktop_impressions) AS desktop_impressions,
        SUM(mobile_impressions) AS mobile_impressions
    FROM `{project_id}.{dataset}.raw_pageviews`
    GROUP BY 1, 2
),
holiday_distances AS (
    SELECT
        p.date,
        p.ad_unit,
        p.daily_impressions,
        p.desktop_impressions,
        p.mobile_impressions,
        h.holiday_date,
        h.holiday_name,
        DATE_DIFF(h.holiday_date, p.date, DAY) AS days_until_holiday
    FROM pageviews_combined p
    LEFT JOIN `{project_id}.{dataset}.us_holidays` h
        ON h.holiday_date >= p.date
),
next_holiday AS (
    SELECT
        date,
        ad_unit,
        daily_impressions,
        desktop_impressions,
        mobile_impressions,
        MIN(days_until_holiday) AS days_to_next_holiday
    FROM holiday_distances
    WHERE days_until_holiday >= 0
    GROUP BY 1, 2, 3, 4, 5
)
SELECT
    p.date,
    p.ad_unit,
    p.daily_impressions,
    p.desktop_impressions,
    p.mobile_impressions,
    EXTRACT(DAYOFWEEK FROM p.date) AS day_of_week,
    EXTRACT(DAYOFWEEK FROM p.date) IN (1, 7) AS is_weekend,
    EXTRACT(QUARTER FROM p.date) AS quarter,
    EXTRACT(WEEK FROM p.date) AS week_of_year,
    h.holiday_date IS NOT NULL AS is_holiday,
    h.holiday_name,
    nh.days_to_next_holiday
FROM pageviews_combined p
LEFT JOIN `{project_id}.{dataset}.us_holidays` h
    ON p.date = h.holiday_date
LEFT JOIN next_holiday nh
    ON p.date = nh.date AND p.ad_unit = nh.ad_unit
ORDER BY p.date, p.ad_unit;
