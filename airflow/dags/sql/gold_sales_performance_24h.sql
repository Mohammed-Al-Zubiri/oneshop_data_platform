CREATE OR REPLACE TABLE iceberg.gold.sales_performance_24h AS
SELECT
    p.purchase_hour AS purchase_hour,
    SUM(p.total_price) AS total_revenue
FROM
    iceberg.silver.purchases_enriched p
WHERE
    p.created_at >= CURRENT_TIMESTAMP - INTERVAL '24' HOUR
GROUP BY
    purchase_hour
ORDER BY
    purchase_hour ASC
