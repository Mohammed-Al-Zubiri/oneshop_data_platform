CREATE OR REPLACE TABLE iceberg.gold.top_selling_items AS
SELECT
    item_id,
    item_name,
    item_category,
    SUM(total_price) AS total_revenue
FROM
    iceberg.silver.purchases_enriched
GROUP BY
    item_id, item_name, item_category
ORDER BY
    total_revenue DESC
LIMIT 10
