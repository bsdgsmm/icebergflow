-- Question: Which ten products earn the most net revenue, and how much of their category do they carry?
SELECT
    product_id,
    product_name,
    category,
    units_sold,
    net_revenue,
    CAST(
        net_revenue / NULLIF(SUM(net_revenue) OVER (PARTITION BY category), 0)
        AS DECIMAL(6, 4)
    ) AS share_of_category,
    category_revenue_rank
FROM gold.product_performance
ORDER BY net_revenue DESC, product_id
LIMIT 10
