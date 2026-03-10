-- Generate forecasts from trained ARIMA_PLUS_XREG model
-- Uses ML.FORECAST with future_features as external regressors
--
-- Important: The regressors subquery MUST include ad_unit because the model
-- was trained with time_series_id_col='ad_unit'. BigQuery ML requires the
-- series ID column in regressors to match values to each time series.
--
-- Atomicity: DELETE then INSERT is not atomic in BigQuery.
-- If INSERT fails after DELETE, data is lost. Re-run to recover.

-- Clear existing forecasts for this model and fold
DELETE FROM `{project_id}.{dataset}.forecasts`
WHERE model_name = 'arima_plus_xreg' AND fold_name = '{fold_name}';

-- Generate and store forecasts with external regressors
INSERT INTO `{project_id}.{dataset}.forecasts` (
    forecast_date, ad_unit, model_name, fold_name,
    forecast_value, forecast_lower, forecast_upper, confidence_level
)
SELECT
    DATE(forecast_timestamp) AS forecast_date,
    ad_unit,
    'arima_plus_xreg' AS model_name,
    '{fold_name}' AS fold_name,
    forecast_value,
    prediction_interval_lower_bound AS forecast_lower,
    prediction_interval_upper_bound AS forecast_upper,
    {confidence_level} AS confidence_level
FROM ML.FORECAST(
    MODEL `{project_id}.{dataset}.arima_plus_xreg_{fold_name}`,
    STRUCT(
        {horizon} AS horizon,
        {confidence_level} AS confidence_level
    ),
    (
        SELECT
            date,
            ad_unit,
            day_of_week,
            CAST(is_weekend AS INT64) AS is_weekend,
            CAST(is_holiday AS INT64) AS is_holiday,
            COALESCE(days_to_next_holiday, 0) AS days_to_next_holiday
        FROM `{project_id}.{dataset}.future_features`
        WHERE date BETWEEN DATE '{test_start}' AND DATE '{test_end}'
    )
);
