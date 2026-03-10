-- Model output tables for walk-forward validation
-- Tables: forecasts, model_metrics, model_comparison, future_features, forecast_decomposition

-- Forecasts storage (all models, all folds)
CREATE TABLE IF NOT EXISTS `{project_id}.{dataset}.forecasts` (
    forecast_date DATE NOT NULL,
    ad_unit STRING NOT NULL,
    model_name STRING NOT NULL,
    fold_name STRING NOT NULL,
    forecast_value FLOAT64 NOT NULL,
    forecast_lower FLOAT64,
    forecast_upper FLOAT64,
    confidence_level FLOAT64
)
PARTITION BY forecast_date
CLUSTER BY ad_unit, model_name;

-- Evaluation metrics per model x fold x ad_unit
CREATE TABLE IF NOT EXISTS `{project_id}.{dataset}.model_metrics` (
    model_name STRING NOT NULL,
    fold_name STRING NOT NULL,
    ad_unit STRING NOT NULL,
    metric_name STRING NOT NULL,
    metric_value FLOAT64 NOT NULL,
    test_start DATE NOT NULL,
    test_end DATE NOT NULL
)
CLUSTER BY model_name, fold_name;

-- Aggregated comparison across ad units
CREATE TABLE IF NOT EXISTS `{project_id}.{dataset}.model_comparison` (
    model_name STRING NOT NULL,
    fold_name STRING NOT NULL,
    metric_name STRING NOT NULL,
    mean_value FLOAT64,
    median_value FLOAT64,
    std_value FLOAT64,
    min_value FLOAT64,
    max_value FLOAT64
);

-- Future calendar features for XREG forecasts
-- Required for providing external regressors during prediction
-- Note: ad_unit is required because ARIMA_PLUS_XREG with time_series_id_col
-- needs regressors matched to each series at forecast time
CREATE TABLE IF NOT EXISTS `{project_id}.{dataset}.future_features` (
    date DATE NOT NULL,
    ad_unit STRING NOT NULL,
    day_of_week INT64 NOT NULL,
    is_weekend BOOL NOT NULL,
    quarter INT64 NOT NULL,
    week_of_year INT64 NOT NULL,
    is_holiday BOOL NOT NULL,
    holiday_name STRING,
    days_to_next_holiday INT64
)
PARTITION BY date
CLUSTER BY ad_unit;

-- Business impact: revenue at risk from forecast error
-- Formula: avg_daily_impressions × CPM × (mape / 100) / 1000
-- Uses $5.50 mid-range CPM (industry benchmark for display inventory)
CREATE TABLE IF NOT EXISTS `{project_id}.{dataset}.business_impact` (
    model_name STRING NOT NULL,
    fold_name STRING NOT NULL,
    ad_unit STRING NOT NULL,
    avg_daily_impressions FLOAT64 NOT NULL,
    mape FLOAT64 NOT NULL,
    cpm FLOAT64 NOT NULL,
    daily_revenue_at_risk FLOAT64 NOT NULL,
    annual_revenue_at_risk FLOAT64 NOT NULL
)
CLUSTER BY model_name, fold_name;

-- ARIMA_PLUS forecast decomposition from ML.EXPLAIN_FORECAST
-- Used for dashboard decomposition tab and model interpretability
CREATE TABLE IF NOT EXISTS `{project_id}.{dataset}.forecast_decomposition` (
    forecast_date DATE NOT NULL,
    ad_unit STRING NOT NULL,
    fold_name STRING NOT NULL,
    -- Point forecast
    forecast_value FLOAT64,
    prediction_interval_lower_bound FLOAT64,
    prediction_interval_upper_bound FLOAT64,
    -- Decomposition components (sum to forecast_value)
    trend FLOAT64,
    seasonal_period_weekly FLOAT64,
    seasonal_period_yearly FLOAT64,
    holiday_effect FLOAT64,
    spikes_and_dips FLOAT64,
    step_changes FLOAT64,
    -- Residual (unexplained variance)
    residual FLOAT64
)
PARTITION BY forecast_date
CLUSTER BY ad_unit, fold_name;

-- Detected anomalies from ML.DETECT_ANOMALIES on ARIMA_PLUS model
-- Used for dashboard anomaly tab and data quality investigation
CREATE TABLE IF NOT EXISTS `{project_id}.{dataset}.anomalies` (
    date DATE NOT NULL,
    ad_unit STRING NOT NULL,
    fold_name STRING NOT NULL,
    daily_impressions FLOAT64 NOT NULL,
    is_anomaly BOOL NOT NULL,
    lower_bound FLOAT64,
    upper_bound FLOAT64,
    anomaly_probability FLOAT64
)
PARTITION BY date
CLUSTER BY ad_unit, is_anomaly;
