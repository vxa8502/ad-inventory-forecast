-- Calculate evaluation metrics by joining forecasts with actuals
-- Metrics: MAPE, RMSE, MAE, MASE, Coverage
--
-- MASE baseline: Computed from training set naive forecast (lag-1 differences).
-- If naive baseline is zero or NULL (constant series), MASE is set to NULL.
-- This is statistically appropriate: MASE is undefined for constant series.
--
-- Atomicity: DELETE then INSERT is not atomic in BigQuery.
-- If INSERT fails after DELETE, data is lost. Re-run to recover.

-- Clear existing metrics for this fold
DELETE FROM `{project_id}.{dataset}.model_metrics`
WHERE fold_name = '{fold_name}';

-- Calculate metrics per model x ad_unit
INSERT INTO `{project_id}.{dataset}.model_metrics` (
    model_name, fold_name, ad_unit, metric_name, metric_value, test_start, test_end
)
WITH actuals AS (
    SELECT date, ad_unit, daily_impressions AS actual
    FROM `{project_id}.{dataset}.daily_impressions`
    WHERE date BETWEEN DATE '{test_start}' AND DATE '{test_end}'
),
forecast_vs_actual AS (
    SELECT
        f.model_name,
        f.fold_name,
        f.ad_unit,
        f.forecast_date,
        f.forecast_value,
        f.forecast_lower,
        f.forecast_upper,
        a.actual
    FROM `{project_id}.{dataset}.forecasts` f
    JOIN actuals a
        ON f.forecast_date = a.date
        AND f.ad_unit = a.ad_unit
    WHERE f.fold_name = '{fold_name}'
),
-- Naive forecast baseline for MASE (previous day's value)
naive_diffs AS (
    SELECT
        ad_unit,
        daily_impressions,
        LAG(daily_impressions) OVER (PARTITION BY ad_unit ORDER BY date) AS prev_impressions
    FROM `{project_id}.{dataset}.daily_impressions`
    WHERE date BETWEEN DATE '{train_start}' AND DATE '{train_end}'
),
naive_errors AS (
    SELECT
        ad_unit,
        AVG(ABS(daily_impressions - prev_impressions)) AS mean_naive_error
    FROM naive_diffs
    WHERE prev_impressions IS NOT NULL
    GROUP BY ad_unit
),
metrics_raw AS (
    SELECT
        fva.model_name,
        fva.fold_name,
        fva.ad_unit,
        -- MAPE: Mean Absolute Percentage Error
        AVG(ABS(fva.forecast_value - fva.actual) / NULLIF(fva.actual, 0)) * 100 AS mape,
        -- RMSE: Root Mean Squared Error
        SQRT(AVG(POW(fva.forecast_value - fva.actual, 2))) AS rmse,
        -- MAE: Mean Absolute Error
        AVG(ABS(fva.forecast_value - fva.actual)) AS mae,
        -- MASE: Mean Absolute Scaled Error (NULL if naive baseline is zero/missing)
        -- ANY_VALUE used because mean_naive_error is 1:1 with ad_unit from LEFT JOIN
        CASE
            WHEN ANY_VALUE(ne.mean_naive_error) IS NULL OR ANY_VALUE(ne.mean_naive_error) = 0 THEN NULL
            ELSE AVG(ABS(fva.forecast_value - fva.actual)) / ANY_VALUE(ne.mean_naive_error)
        END AS mase,
        -- Coverage: % of actuals within prediction interval
        AVG(CASE
            WHEN fva.actual BETWEEN fva.forecast_lower AND fva.forecast_upper THEN 1.0
            ELSE 0.0
        END) * 100 AS coverage
    FROM forecast_vs_actual fva
    LEFT JOIN naive_errors ne ON fva.ad_unit = ne.ad_unit
    GROUP BY fva.model_name, fva.fold_name, fva.ad_unit
)
SELECT model_name, fold_name, ad_unit, 'mape' AS metric_name, mape AS metric_value,
    DATE '{test_start}' AS test_start, DATE '{test_end}' AS test_end
FROM metrics_raw
UNION ALL
SELECT model_name, fold_name, ad_unit, 'rmse', rmse,
    DATE '{test_start}', DATE '{test_end}'
FROM metrics_raw
UNION ALL
SELECT model_name, fold_name, ad_unit, 'mae', mae,
    DATE '{test_start}', DATE '{test_end}'
FROM metrics_raw
UNION ALL
SELECT model_name, fold_name, ad_unit, 'mase', mase,
    DATE '{test_start}', DATE '{test_end}'
FROM metrics_raw
UNION ALL
SELECT model_name, fold_name, ad_unit, 'coverage', coverage,
    DATE '{test_start}', DATE '{test_end}'
FROM metrics_raw;
