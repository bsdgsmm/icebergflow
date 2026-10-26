-- Question: How much more are repeat customers worth than one-time buyers?
SELECT
    CASE WHEN is_repeat_customer THEN 'repeat' ELSE 'one_time' END AS segment,
    COUNT(*) AS customers,
    CAST(AVG(order_count) AS DECIMAL(8, 2)) AS avg_orders,
    CAST(AVG(lifetime_revenue) AS DECIMAL(12, 2)) AS avg_lifetime_revenue,
    CAST(AVG(avg_order_value) AS DECIMAL(12, 2)) AS avg_order_value,
    CAST(SUM(lifetime_revenue) / SUM(SUM(lifetime_revenue)) OVER () AS DECIMAL(6, 4))
        AS revenue_share
FROM gold.customer_summary
WHERE order_count > 0
GROUP BY CASE WHEN is_repeat_customer THEN 'repeat' ELSE 'one_time' END
ORDER BY segment DESC
