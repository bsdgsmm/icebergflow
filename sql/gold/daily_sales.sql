-- gold.daily_sales
-- Grain: one row per order_date (UTC).
-- Net figures include orders whose current status is placed, shipped or
-- delivered. Cancelled and returned orders count towards gross revenue and are
-- reported separately. Orders whose lines were all quarantined contribute 0.
WITH order_totals AS (
    SELECT
        o.order_id,
        o.customer_id,
        o.order_date,
        o.status,
        o.status IN ('placed', 'shipped', 'delivered') AS is_net,
        COALESCE(SUM(i.quantity), 0) AS units,
        COALESCE(SUM(i.line_amount), 0) AS amount
    FROM silver.orders AS o
    LEFT JOIN silver.order_items AS i ON i.order_id = o.order_id
    GROUP BY o.order_id, o.customer_id, o.order_date, o.status
),

daily AS (
    SELECT
        order_date,
        COUNT(*) AS orders,
        COUNT_IF(is_net) AS net_orders,
        COUNT_IF(status = 'cancelled') AS cancelled_orders,
        COUNT_IF(status = 'returned') AS returned_orders,
        COUNT(DISTINCT customer_id) AS unique_customers,
        SUM(CASE WHEN is_net THEN units ELSE 0 END) AS units_sold,
        SUM(amount) AS gross_revenue,
        SUM(CASE WHEN is_net THEN amount ELSE 0 END) AS net_revenue
    FROM order_totals
    GROUP BY order_date
)

SELECT
    order_date,
    orders,
    net_orders,
    cancelled_orders,
    returned_orders,
    unique_customers,
    CAST(units_sold AS BIGINT) AS units_sold,
    CAST(gross_revenue AS DECIMAL(14, 2)) AS gross_revenue,
    CAST(net_revenue AS DECIMAL(14, 2)) AS net_revenue,
    CAST(net_revenue / NULLIF(net_orders, 0) AS DECIMAL(12, 2)) AS avg_order_value
FROM daily
