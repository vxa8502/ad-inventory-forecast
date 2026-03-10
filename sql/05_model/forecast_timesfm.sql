-- Generate forecasts using TimesFM 2.5 (zero-shot foundation model)
-- No training required; uses AI.FORECAST with pre-trained TimesFM
--
-- Prerequisites:
--   - BigQuery Enterprise edition OR BigQuery Enterprise Plus
--   - AI in BigQuery features enabled for the project
--   - See: https://cloud.google.com/bigquery/docs/ai-forecast
--
-- Atomicity: DELETE then INSERT is not atomic in BigQuery.
-- If INSERT fails after DELETE, data is lost. Re-run to recover.

-- Clear existing forecasts for this model and fold
DELETE FROM `{project_id}.{dataset}.forecasts`
WHERE model_name = 'timesfm_2_5' AND fold_name = '{fold_name}';

-- Generate and store forecasts
INSERT INTO `{project_id}.{dataset}.forecasts` (
    forecast_date, ad_unit, model_name, fold_name,
    forecast_value, forecast_lower, forecast_upper, confidence_level
)
SELECT
    DATE(forecast_timestamp) AS forecast_date,
    ad_unit,
    'timesfm_2_5' AS model_name,
    '{fold_name}' AS fold_name,
    forecast_value,
    prediction_interval_lower_bound AS forecast_lower,
    prediction_interval_upper_bound AS forecast_upper,
    {confidence_level} AS confidence_level
FROM AI.FORECAST(
    (SELECT date, ad_unit, daily_impressions
     FROM `{project_id}.{dataset}.daily_impressions`
     WHERE date BETWEEN DATE '{train_start}' AND DATE '{train_end}'),
    DATA_COL => 'daily_impressions',
    TIMESTAMP_COL => 'date',
    ID_COLS => ['ad_unit'],
    MODEL => 'TimesFM 2.5',
    HORIZON => {horizon},
    CONFIDENCE_LEVEL => {confidence_level}
);
