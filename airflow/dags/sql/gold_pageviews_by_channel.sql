CREATE OR REPLACE TABLE iceberg.gold.pageviews_by_channel AS
SELECT
    channel,
    COUNT(*) AS total_pageviews
FROM
    iceberg.silver.pageviews_by_items
GROUP BY
    channel
ORDER BY
    total_pageviews DESC
