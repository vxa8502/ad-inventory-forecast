-- Extract ARIMA model evaluation and coefficients for inspection
-- Implements Aria's checklist: inspect AIC scores and (p,d,q) orders
--
-- ML.ARIMA_EVALUATE provides (p,d,q) orders, AIC, and detection flags
-- ML.ARIMA_COEFFICIENTS provides fitted AR/MA coefficient values

-- Step 1: Get ARIMA coefficients (AR/MA values)
-- Note: ML.ARIMA_COEFFICIENTS returns ar_coefficients, ma_coefficients,
-- and intercept_or_drift. The (p,d,q) orders come from ML.ARIMA_EVALUATE.
CREATE OR REPLACE TABLE `{project_id}.{dataset}.arima_coefficients_{fold_name}` AS
SELECT
    ad_unit,
    ar_coefficients,
    ma_coefficients,
    intercept_or_drift
FROM ML.ARIMA_COEFFICIENTS(
    MODEL `{project_id}.{dataset}.arima_plus_{fold_name}`
);

-- Step 2: Get model evaluation metrics (AIC, variance, etc.)
CREATE OR REPLACE TABLE `{project_id}.{dataset}.arima_evaluate_{fold_name}` AS
SELECT
    ad_unit,
    non_seasonal_p,
    non_seasonal_d,
    non_seasonal_q,
    CONCAT('ARIMA(', non_seasonal_p, ',', non_seasonal_d, ',', non_seasonal_q, ')') AS arima_order,
    has_drift,
    log_likelihood,
    AIC,
    variance,
    -- Seasonal order (if detected)
    seasonal_periods,
    has_holiday_effect,
    has_spikes_and_dips,
    has_step_changes
FROM ML.ARIMA_EVALUATE(
    MODEL `{project_id}.{dataset}.arima_plus_{fold_name}`
);

-- Step 3: Summary view for hypothesis validation
-- Groups articles by ARIMA order to identify patterns
CREATE OR REPLACE TABLE `{project_id}.{dataset}.arima_order_summary_{fold_name}` AS
WITH order_groups AS (
    SELECT
        arima_order,
        non_seasonal_d,
        has_drift,
        COUNT(*) AS article_count,
        ARRAY_AGG(ad_unit ORDER BY ad_unit) AS articles,
        AVG(AIC) AS avg_aic,
        MIN(AIC) AS min_aic,
        MAX(AIC) AS max_aic
    FROM `{project_id}.{dataset}.arima_evaluate_{fold_name}`
    GROUP BY arima_order, non_seasonal_d, has_drift
)
SELECT
    arima_order,
    non_seasonal_d AS differencing_order,
    has_drift,
    article_count,
    articles,
    ROUND(avg_aic, 2) AS avg_aic,
    ROUND(min_aic, 2) AS min_aic,
    ROUND(max_aic, 2) AS max_aic
FROM order_groups
ORDER BY article_count DESC;
