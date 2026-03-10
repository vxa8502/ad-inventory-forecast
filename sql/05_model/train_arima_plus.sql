-- Train ARIMA_PLUS model for a specific fold
-- Uses BigQuery ML built-in holiday detection (holiday_region = 'US')

CREATE OR REPLACE MODEL `{project_id}.{dataset}.arima_plus_{fold_name}`
OPTIONS(
    model_type = 'ARIMA_PLUS',
    time_series_timestamp_col = 'date',
    time_series_data_col = 'daily_impressions',
    time_series_id_col = 'ad_unit',
    data_frequency = 'DAILY',
    holiday_region = 'US',
    clean_spikes_and_dips = TRUE,
    adjust_step_changes = TRUE,
    decompose_time_series = TRUE,
    auto_arima = TRUE
) AS
SELECT date, ad_unit, daily_impressions
FROM `{project_id}.{dataset}.daily_impressions`
WHERE date BETWEEN DATE '{train_start}' AND DATE '{train_end}';
