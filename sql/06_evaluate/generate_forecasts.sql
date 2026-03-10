-- Generate forecasts from trained ARIMA_PLUS model
-- Uses ML.FORECAST to predict for the test horizon
--
-- Atomicity: DELETE then INSERT is not atomic in BigQuery.
-- If INSERT fails after DELETE, data is lost. Re-run to recover.

-- Clear existing forecasts for this model and fold
DELETE FROM `{project_id}.{dataset}.forecasts`
WHERE model_name = 'arima_plus' AND fold_name = '{fold_name}';

-- Generate and store forecasts
INSERT INTO `{project_id}.{dataset}.forecasts` (
    forecast_date, ad_unit, model_name, fold_name,
    forecast_value, forecast_lower, forecast_upper, confidence_level
)
SELECT
    DATE(forecast_timestamp) AS forecast_date,
    ad_unit,
    'arima_plus' AS model_name,
    '{fold_name}' AS fold_name,
    forecast_value,
    prediction_interval_lower_bound AS forecast_lower,
    prediction_interval_upper_bound AS forecast_upper,
    {confidence_level} AS confidence_level
FROM ML.FORECAST(
    MODEL `{project_id}.{dataset}.arima_plus_{fold_name}`,
    STRUCT(
        {horizon} AS horizon,
        {confidence_level} AS confidence_level
    )
);
