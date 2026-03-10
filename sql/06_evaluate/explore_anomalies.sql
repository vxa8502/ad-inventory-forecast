-- Ad-hoc query to explore detected anomalies
-- Run this interactively to cross-reference with real-world events
--
-- Usage: Copy to BigQuery console and modify filters as needed

-- Top anomalies by probability (most confident detections)
SELECT
    date,
    ad_unit,
    fold_name,
    ROUND(daily_impressions, 0) AS impressions,
    ROUND(lower_bound, 0) AS expected_lower,
    ROUND(upper_bound, 0) AS expected_upper,
    ROUND(anomaly_probability, 4) AS probability,
    CASE
        WHEN daily_impressions > upper_bound THEN 'HIGH_SPIKE'
        WHEN daily_impressions < lower_bound THEN 'LOW_DIP'
        ELSE 'UNKNOWN'
    END AS anomaly_type,
    ROUND((daily_impressions - (lower_bound + upper_bound) / 2) /
          NULLIF((upper_bound - lower_bound) / 2, 0), 2) AS deviation_ratio
FROM `{project_id}.{dataset}.anomalies`
WHERE is_anomaly = TRUE
ORDER BY anomaly_probability DESC, date
LIMIT 100;


-- Anomalies by date (for event cross-referencing)
SELECT
    date,
    COUNT(*) AS articles_affected,
    ARRAY_AGG(ad_unit ORDER BY anomaly_probability DESC) AS affected_articles,
    ROUND(AVG(anomaly_probability), 4) AS avg_probability
FROM `{project_id}.{dataset}.anomalies`
WHERE is_anomaly = TRUE
GROUP BY date
HAVING COUNT(*) >= 3  -- Multiple articles affected = likely real event
ORDER BY articles_affected DESC, date
LIMIT 50;


-- Anomaly rate by article (which content is most volatile?)
SELECT
    ad_unit,
    COUNT(*) AS total_days,
    COUNTIF(is_anomaly) AS anomaly_days,
    ROUND(COUNTIF(is_anomaly) / COUNT(*) * 100, 1) AS anomaly_rate_pct,
    ROUND(AVG(CASE WHEN is_anomaly THEN anomaly_probability END), 4) AS avg_anomaly_probability
FROM `{project_id}.{dataset}.anomalies`
GROUP BY ad_unit
ORDER BY anomaly_rate_pct DESC;


-- Super Bowl Sunday check (expected high-confidence anomaly)
SELECT
    date,
    ad_unit,
    ROUND(daily_impressions, 0) AS impressions,
    ROUND(anomaly_probability, 4) AS probability,
    is_anomaly
FROM `{project_id}.{dataset}.anomalies`
WHERE ad_unit IN ('NFL', 'Super_Bowl')
  AND EXTRACT(MONTH FROM date) = 2
  AND EXTRACT(DAYOFWEEK FROM date) = 1  -- Sunday
ORDER BY date DESC;


-- Barbenheimer weekend check (July 21-23, 2023)
SELECT
    date,
    ad_unit,
    ROUND(daily_impressions, 0) AS impressions,
    ROUND(anomaly_probability, 4) AS probability,
    is_anomaly
FROM `{project_id}.{dataset}.anomalies`
WHERE ad_unit IN ('Barbie_(film)', 'Oppenheimer_(film)')
  AND date BETWEEN '2023-07-15' AND '2023-07-31'
ORDER BY ad_unit, date;
