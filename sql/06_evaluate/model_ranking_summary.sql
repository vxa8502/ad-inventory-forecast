-- Model ranking summary for README Results section
-- Ranks models by average MAPE across all series (cross-fold average)
--
-- Run this query ad-hoc to generate the results table for documentation.
-- No INSERT: this is a reporting query, not a pipeline step.

WITH mape_ranking AS (
    SELECT
        model_name,
        mean_value AS mean_mape,
        median_value AS median_mape,
        std_value AS mape_std,
        RANK() OVER (ORDER BY mean_value ASC) AS rank
    FROM `{project_id}.{dataset}.model_comparison`
    WHERE fold_name = 'cross_fold_avg'
      AND metric_name = 'mape'
),
rmse_values AS (
    SELECT model_name, mean_value AS mean_rmse
    FROM `{project_id}.{dataset}.model_comparison`
    WHERE fold_name = 'cross_fold_avg'
      AND metric_name = 'rmse'
),
coverage_values AS (
    SELECT model_name, mean_value AS mean_coverage
    FROM `{project_id}.{dataset}.model_comparison`
    WHERE fold_name = 'cross_fold_avg'
      AND metric_name = 'coverage'
),
business_summary AS (
    SELECT
        model_name,
        SUM(daily_revenue_at_risk) AS total_daily_risk,
        SUM(annual_revenue_at_risk) AS total_annual_risk
    FROM `{project_id}.{dataset}.business_impact`
    WHERE fold_name IN ('fold_1', 'fold_2')
    GROUP BY model_name
)
SELECT
    m.rank,
    m.model_name,
    ROUND(m.mean_mape, 2) AS mean_mape_pct,
    ROUND(m.median_mape, 2) AS median_mape_pct,
    ROUND(r.mean_rmse, 0) AS mean_rmse,
    ROUND(c.mean_coverage, 1) AS coverage_95_pct,
    ROUND(b.total_daily_risk, 2) AS daily_revenue_at_risk_usd,
    ROUND(b.total_annual_risk, 0) AS annual_revenue_at_risk_usd
FROM mape_ranking m
LEFT JOIN rmse_values r ON m.model_name = r.model_name
LEFT JOIN coverage_values c ON m.model_name = c.model_name
LEFT JOIN business_summary b ON m.model_name = b.model_name
ORDER BY m.rank;

-- Expected output format for README:
-- | Rank | Model           | Mean MAPE | Median MAPE | RMSE    | Coverage | Daily Risk | Annual Risk |
-- |------|-----------------|-----------|-------------|---------|----------|------------|-------------|
-- | 1    | TIMESFM_2_5     | 20.35%    | 18.21%      | 45,230  | 89.2%    | $127.50    | $46,538     |
-- | 2    | ARIMA_PLUS      | 41.45%    | 38.92%      | 62,108  | 91.5%    | $259.80    | $94,827     |
-- | 3    | ARIMA_PLUS_XREG | 76.75%    | 52.34%      | 98,442  | 78.3%    | $480.25    | $175,291    |
