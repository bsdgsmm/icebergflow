-- Question: Which countries bring the most buyers and revenue, and how loyal are they?
SELECT
    country,
    COUNT(*) AS buyers,
    COUNT_IF(is_repeat_customer) AS repeat_buyers,
    CAST(COUNT_IF(is_repeat_customer) / COUNT(*) AS DECIMAL(6, 4)) AS repeat_rate,
    CAST(SUM(lifetime_revenue) AS DECIMAL(14, 2)) AS net_revenue,
    CAST(SUM(lifetime_revenue) / COUNT(*) AS DECIMAL(12, 2)) AS revenue_per_buyer
FROM gold.customer_summary
WHERE order_count > 0
GROUP BY country
ORDER BY net_revenue DESC
