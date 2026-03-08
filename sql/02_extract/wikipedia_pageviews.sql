-- Clear existing data for idempotent re-runs
DELETE FROM `{project_id}.{dataset}.raw_pageviews` WHERE TRUE;

INSERT INTO `{project_id}.{dataset}.raw_pageviews` (
    date,
    wiki,
    ad_unit,
    daily_impressions,
    desktop_impressions,
    mobile_impressions
)
WITH combined_pageviews AS (
    -- 2023 data
    SELECT datehour, wiki, title, views
    FROM `bigquery-public-data.wikipedia.pageviews_2023`
    WHERE
        datehour BETWEEN TIMESTAMP('2023-01-01') AND TIMESTAMP('2023-12-31 23:59:59')
        AND title IN ({article_list})
        AND wiki IN ('en', 'en.m')
    UNION ALL
    -- 2024 data
    SELECT datehour, wiki, title, views
    FROM `bigquery-public-data.wikipedia.pageviews_2024`
    WHERE
        datehour BETWEEN TIMESTAMP('2024-01-01') AND TIMESTAMP('2024-12-31 23:59:59')
        AND title IN ({article_list})
        AND wiki IN ('en', 'en.m')
)
SELECT
    DATE(datehour) AS date,
    'en' AS wiki,
    title AS ad_unit,
    SUM(views) AS daily_impressions,
    SUM(CASE WHEN wiki = 'en' THEN views ELSE 0 END) AS desktop_impressions,
    SUM(CASE WHEN wiki = 'en.m' THEN views ELSE 0 END) AS mobile_impressions
FROM combined_pageviews
GROUP BY 1, 3
ORDER BY 1, 3;
