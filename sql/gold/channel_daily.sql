-- gold.channel_daily
-- Grain: one row per (order_date, channel). channel and coupon_code arrived
-- with v2 of the order feed; orders placed before that report channel
-- 'unknown'. Net figures follow the same status rules as gold.daily_sales.
WITH order_totals AS (
    SELECT
        o.order_id,
        o.order_date,
        COALESCE(o.channel, 'unknown') AS channel,
        o.coupon_code,
        COALESCE(SUM(i.line_amount), 0) AS amount
    FROM silver.orders AS o
    LEFT JOIN silver.order_items AS i ON i.order_id = o.order_id
    WHERE o.status IN ('placed', 'shipped', 'delivered')
    GROUP BY o.order_id, o.order_date, o.channel, o.coupon_code
)

SELECT
    order_date,
    channel,
    COUNT(*) AS net_orders,
    COUNT_IF(coupon_code IS NOT NULL) AS coupon_orders,
    CAST(SUM(amount) AS DECIMAL(14, 2)) AS net_revenue,
    CAST(AVG(amount) AS DECIMAL(12, 2)) AS avg_order_value
FROM order_totals
GROUP BY order_date, channel
