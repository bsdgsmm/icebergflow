-- Question: How does each category contribute to monthly net revenue?
SELECT
    order_month,
    category,
    orders,
    units_sold,
    net_revenue,
    revenue_share,
    RANK() OVER (PARTITION BY order_month ORDER BY net_revenue DESC) AS rank_in_month
FROM gold.category_monthly
ORDER BY order_month, rank_in_month, category
