-- Raw pageviews table
-- wiki: Always 'en' after aggregation (reserved for future language expansion)
-- Device split: desktop_impressions from en wiki, mobile_impressions from en.m wiki
CREATE TABLE IF NOT EXISTS `{project_id}.{dataset}.raw_pageviews` (
    date DATE NOT NULL,
    wiki STRING NOT NULL,
    ad_unit STRING NOT NULL,
    daily_impressions INT64 NOT NULL,
    desktop_impressions INT64 NOT NULL,
    mobile_impressions INT64 NOT NULL
)
PARTITION BY date
CLUSTER BY ad_unit
OPTIONS (
    description = 'Raw Wikipedia pageviews aggregated to daily granularity'
);

CREATE TABLE IF NOT EXISTS `{project_id}.{dataset}.us_holidays` (
    holiday_date DATE NOT NULL,
    holiday_name STRING NOT NULL,
    is_major BOOL NOT NULL
)
OPTIONS (
    description = 'US federal and commercial holidays for feature engineering'
);

-- Feature-enriched table for forecasting
-- day_of_week: BigQuery DAYOFWEEK convention (1=Sunday, 7=Saturday)
-- days_to_next_holiday: 0 when is_holiday=TRUE (today is the holiday)
CREATE TABLE IF NOT EXISTS `{project_id}.{dataset}.daily_impressions` (
    date DATE NOT NULL,
    ad_unit STRING NOT NULL,
    daily_impressions INT64 NOT NULL,
    desktop_impressions INT64 NOT NULL,
    mobile_impressions INT64 NOT NULL,
    day_of_week INT64 NOT NULL,
    is_weekend BOOL NOT NULL,
    quarter INT64 NOT NULL,
    week_of_year INT64 NOT NULL,
    is_holiday BOOL NOT NULL,
    holiday_name STRING,
    days_to_next_holiday INT64
)
PARTITION BY date
CLUSTER BY ad_unit
OPTIONS (
    description = 'Feature-enriched daily impressions for forecasting models'
);
