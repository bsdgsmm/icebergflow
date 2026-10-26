-- Question: How is daily net revenue trending, smoothed over a 7-day window?
SELECT
    order_date,
    net_orders,
    net_revenue,
    CAST(
        AVG(net_revenue) OVER (ORDER BY order_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
        AS DECIMAL(14, 2)
    ) AS net_revenue_7d_avg,
    CAST(
        (net_revenue - LAG(net_revenue) OVER (ORDER BY order_date))
        / NULLIF(LAG(net_revenue) OVER (ORDER BY order_date), 0)
        AS DECIMAL(8, 4)
    ) AS day_over_day_change,
    CAST(cancelled_orders / NULLIF(orders, 0) AS DECIMAL(6, 4)) AS cancellation_rate
FROM gold.daily_sales
ORDER BY order_date
