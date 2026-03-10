-- Generate and store forecast decomposition using ML.EXPLAIN_FORECAST
-- Required for dashboard decomposition tab and interview walkthroughs
--
-- Output columns:
--   - trend: Long-term direction
--   - seasonal_period_weekly: 7-day cycle component
--   - seasonal_period_yearly: 365-day cycle component
--   - holiday_effect: US holiday impact
--   - spikes_and_dips: Cleaned outlier adjustments
--   - step_changes: Level shift adjustments

-- Create decomposition storage table (if not exists)
CREATE TABLE IF NOT EXISTS `{project_id}.{dataset}.forecast_decomposition` (
    forecast_date DATE NOT NULL,
    ad_unit STRING NOT NULL,
    fold_name STRING NOT NULL,
    -- Point forecast
    forecast_value FLOAT64,
    prediction_interval_lower_bound FLOAT64,
    prediction_interval_upper_bound FLOAT64,
    -- Decomposition components
    trend FLOAT64,
    seasonal_period_weekly FLOAT64,
    seasonal_period_yearly FLOAT64,
    holiday_effect FLOAT64,
    spikes_and_dips FLOAT64,
    step_changes FLOAT64,
    -- Residual (unexplained)
    residual FLOAT64
)
PARTITION BY forecast_date
CLUSTER BY ad_unit, fold_name;

-- Clear existing decomposition for this fold
DELETE FROM `{project_id}.{dataset}.forecast_decomposition`
WHERE fold_name = '{fold_name}';

-- Generate and store decomposition
INSERT INTO `{project_id}.{dataset}.forecast_decomposition` (
    forecast_date,
    ad_unit,
    fold_name,
    forecast_value,
    prediction_interval_lower_bound,
    prediction_interval_upper_bound,
    trend,
    seasonal_period_weekly,
    seasonal_period_yearly,
    holiday_effect,
    spikes_and_dips,
    step_changes,
    residual
)
SELECT
    DATE(time_series_timestamp) AS forecast_date,
    ad_unit,
    '{fold_name}' AS fold_name,
    forecast_value,
    prediction_interval_lower_bound,
    prediction_interval_upper_bound,
    trend,
    seasonal_period_weekly,
    seasonal_period_yearly,
    holiday_effect,
    spikes_and_dips,
    step_changes,
    -- Residual = forecast - (sum of components)
    forecast_value - COALESCE(trend, 0)
                   - COALESCE(seasonal_period_weekly, 0)
                   - COALESCE(seasonal_period_yearly, 0)
                   - COALESCE(holiday_effect, 0)
                   - COALESCE(spikes_and_dips, 0)
                   - COALESCE(step_changes, 0) AS residual
FROM ML.EXPLAIN_FORECAST(
    MODEL `{project_id}.{dataset}.arima_plus_{fold_name}`,
    STRUCT(
        {horizon} AS horizon,
        {confidence_level} AS confidence_level
    )
);
