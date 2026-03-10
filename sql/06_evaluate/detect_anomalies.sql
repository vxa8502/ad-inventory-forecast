-- Detect anomalies in training data using ARIMA_PLUS model
-- Runs ML.DETECT_ANOMALIES on the trained model to flag unusual traffic days
--
-- Anomalies indicate:
-- - Viral events (movie releases, breaking news)
-- - Data quality issues (collection gaps, bot traffic)
-- - Seasonal extremes (Super Bowl, Black Friday)
--
-- Cross-reference flagged dates with real-world events for "data detective" insights.
--
-- Atomicity: DELETE then INSERT is not atomic in BigQuery.
-- If INSERT fails after DELETE, data is lost. Re-run to recover.

-- Clear existing anomalies for this fold
DELETE FROM `{project_id}.{dataset}.anomalies`
WHERE fold_name = '{fold_name}';

-- Detect anomalies using ARIMA_PLUS model
INSERT INTO `{project_id}.{dataset}.anomalies` (
    date, ad_unit, fold_name,
    daily_impressions, is_anomaly,
    lower_bound, upper_bound, anomaly_probability
)
SELECT
    DATE(date) AS date,
    ad_unit,
    '{fold_name}' AS fold_name,
    daily_impressions,
    is_anomaly,
    lower_bound,
    upper_bound,
    anomaly_probability
FROM ML.DETECT_ANOMALIES(
    MODEL `{project_id}.{dataset}.arima_plus_{fold_name}`,
    STRUCT(0.95 AS anomaly_prob_threshold)
);
