-- Calculate business impact: revenue at risk from forecast error
-- Formula: avg_daily_impressions x CPM x (mape / 100) / 1000
--
-- CPM = Cost Per Mille (per 1000 impressions)
-- Using $5.50 mid-range CPM (industry benchmark for display inventory: $3-$8)
--
-- This transforms abstract MAPE into concrete dollar amounts:
-- "Model B reduces revenue risk by $X/day vs. baseline"
--
-- Atomicity: DELETE then INSERT is not atomic in BigQuery.
-- If INSERT fails after DELETE, data is lost. Re-run to recover.

-- Clear existing impact calculations for this fold
DELETE FROM `{project_id}.{dataset}.business_impact`
WHERE fold_name = '{fold_name}';

-- Calculate revenue at risk per model x ad_unit
INSERT INTO `{project_id}.{dataset}.business_impact` (
    model_name, fold_name, ad_unit,
    avg_daily_impressions, mape, cpm,
    daily_revenue_at_risk, annual_revenue_at_risk
)
WITH avg_impressions AS (
    SELECT
        ad_unit,
        AVG(daily_impressions) AS avg_daily_impressions
    FROM `{project_id}.{dataset}.daily_impressions`
    WHERE date BETWEEN DATE '{train_start}' AND DATE '{train_end}'
    GROUP BY ad_unit
),
mape_values AS (
    SELECT
        model_name,
        fold_name,
        ad_unit,
        metric_value AS mape
    FROM `{project_id}.{dataset}.model_metrics`
    WHERE fold_name = '{fold_name}'
      AND metric_name = 'mape'
)
SELECT
    m.model_name,
    m.fold_name,
    m.ad_unit,
    a.avg_daily_impressions,
    m.mape,
    5.50 AS cpm,
    -- Daily revenue at risk = impressions/1000 * CPM * error_rate
    (a.avg_daily_impressions / 1000) * 5.50 * (m.mape / 100) AS daily_revenue_at_risk,
    -- Annual = daily * 365
    (a.avg_daily_impressions / 1000) * 5.50 * (m.mape / 100) * 365 AS annual_revenue_at_risk
FROM mape_values m
JOIN avg_impressions a ON m.ad_unit = a.ad_unit;
