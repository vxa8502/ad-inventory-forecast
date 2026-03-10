-- Aggregate metrics across walk-forward validation folds
-- Computes the "headline number" by averaging per-fold metrics
--
-- This produces the final model comparison used for reporting:
-- e.g., TimesFM mean MAPE = (fold_1 MAPE + fold_2 MAPE) / 2
--
-- Atomicity: DELETE then INSERT is not atomic in BigQuery.
-- If INSERT fails after DELETE, data is lost. Re-run to recover.

-- Clear existing cross-fold aggregates
DELETE FROM `{project_id}.{dataset}.model_comparison`
WHERE fold_name = 'cross_fold_avg';

-- Average metrics across all folds
INSERT INTO `{project_id}.{dataset}.model_comparison` (
    model_name, fold_name, metric_name,
    mean_value, median_value, std_value, min_value, max_value
)
SELECT
    model_name,
    'cross_fold_avg' AS fold_name,
    metric_name,
    AVG(mean_value) AS mean_value,
    AVG(median_value) AS median_value,
    AVG(std_value) AS std_value,
    MIN(min_value) AS min_value,
    MAX(max_value) AS max_value
FROM `{project_id}.{dataset}.model_comparison`
WHERE fold_name != 'cross_fold_avg'
GROUP BY model_name, metric_name;
