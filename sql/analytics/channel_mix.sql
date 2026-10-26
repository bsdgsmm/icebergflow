-- Question: Which sales channels drive net revenue, and how often are coupons used on each?
SELECT
    channel,
    SUM(net_orders) AS net_orders,
    CAST(SUM(net_revenue) AS DECIMAL(14, 2)) AS net_revenue,
    CAST(SUM(net_revenue) / SUM(SUM(net_revenue)) OVER () AS DECIMAL(6, 4)) AS revenue_share,
    CAST(SUM(net_revenue) / NULLIF(SUM(net_orders), 0) AS DECIMAL(12, 2)) AS avg_order_value,
    CAST(SUM(coupon_orders) / NULLIF(SUM(net_orders), 0) AS DECIMAL(6, 4)) AS coupon_rate
FROM gold.channel_daily
GROUP BY channel
ORDER BY net_revenue DESC
