-- Aggregate metrics across ad units for model comparison
-- Produces summary statistics per model x fold x metric
--
-- NULL handling: MASE metric can be NULL for constant series (naive baseline = 0).
-- All aggregate functions (AVG, APPROX_QUANTILES, STDDEV, MIN, MAX) ignore NULLs
-- by default in BigQuery, so statistics are computed over non-NULL values only.
--
-- Atomicity: DELETE then INSERT is not atomic in BigQuery.
-- If INSERT fails after DELETE, data is lost. Re-run to recover.

-- Clear existing comparison for this fold
DELETE FROM `{project_id}.{dataset}.model_comparison`
WHERE fold_name = '{fold_name}';

-- Aggregate metrics
INSERT INTO `{project_id}.{dataset}.model_comparison` (
    model_name, fold_name, metric_name,
    mean_value, median_value, std_value, min_value, max_value
)
SELECT
    model_name,
    fold_name,
    metric_name,
    AVG(metric_value) AS mean_value,
    APPROX_QUANTILES(metric_value, 2)[OFFSET(1)] AS median_value,
    STDDEV(metric_value) AS std_value,
    MIN(metric_value) AS min_value,
    MAX(metric_value) AS max_value
FROM `{project_id}.{dataset}.model_metrics`
WHERE fold_name = '{fold_name}'
GROUP BY model_name, fold_name, metric_name;
