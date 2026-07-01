CREATE OR REPLACE TABLE iceberg.gold.top_converting_items AS
SELECT
    pvi.item_id,
    pvi.item_name,
    pvi.item_category,
    COUNT(DISTINCT pvi.user_id) AS unique_pageview_users,
    COUNT(DISTINCT pe.user_id) AS unique_purchase_users,
    COUNT(pe.id) AS total_purchases,
    COUNT(pvi.user_id) AS total_pageviews,
    CASE 
        WHEN COUNT(pvi.user_id) = 0 THEN 0
        ELSE CAST(COUNT(pe.id) AS DOUBLE) / COUNT(pvi.user_id)
    END AS conversion_rate
FROM
    iceberg.silver.pageviews_by_items pvi
LEFT JOIN
    iceberg.silver.purchases_enriched pe
    ON pvi.item_id = pe.item_id
    AND pvi.user_id = pe.user_id
    AND date(pvi.received_at) = pe.purchase_date
GROUP BY
    pvi.item_id,
    pvi.item_name,
    pvi.item_category
ORDER BY
    conversion_rate DESC
LIMIT 10
