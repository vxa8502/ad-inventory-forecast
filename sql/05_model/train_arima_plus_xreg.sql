-- Train ARIMA_PLUS_XREG model with calendar features for a specific fold
-- External regressors: day_of_week, is_weekend, is_holiday, days_to_next_holiday

CREATE OR REPLACE MODEL `{project_id}.{dataset}.arima_plus_xreg_{fold_name}`
OPTIONS(
    model_type = 'ARIMA_PLUS_XREG',
    time_series_timestamp_col = 'date',
    time_series_data_col = 'daily_impressions',
    time_series_id_col = 'ad_unit',
    holiday_region = 'US',
    auto_arima = TRUE
) AS
SELECT
    date,
    ad_unit,
    daily_impressions,
    day_of_week,
    CAST(is_weekend AS INT64) AS is_weekend,
    CAST(is_holiday AS INT64) AS is_holiday,
    COALESCE(days_to_next_holiday, 0) AS days_to_next_holiday
FROM `{project_id}.{dataset}.daily_impressions`
WHERE date BETWEEN DATE '{train_start}' AND DATE '{train_end}';
