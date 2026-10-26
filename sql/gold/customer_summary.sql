-- gold.customer_summary
-- Grain: one row per customer_id (every Silver customer, including those who
-- never ordered). Order metrics use net orders only (placed, shipped,
-- delivered), so a cancelled or returned order does not make a repeat buyer.
WITH order_totals AS (
    SELECT
        o.order_id,
        o.customer_id,
        o.order_ts,
        o.order_date,
        COALESCE(SUM(i.line_amount), 0) AS amount
    FROM silver.orders AS o
    LEFT JOIN silver.order_items AS i ON i.order_id = o.order_id
    WHERE o.status IN ('placed', 'shipped', 'delivered')
    GROUP BY o.order_id, o.customer_id, o.order_ts, o.order_date
),

sequenced AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_ts, order_id) AS order_seq
    FROM order_totals
),

per_customer AS (
    SELECT
        customer_id,
        COUNT(*) AS order_count,
        SUM(amount) AS lifetime_revenue,
        MIN(order_date) AS first_order_date,
        MAX(order_date) AS last_order_date,
        MAX(CASE WHEN order_seq = 2 THEN order_date END) AS second_order_date
    FROM sequenced
    GROUP BY customer_id
)

SELECT
    c.customer_id,
    c.country,
    c.signup_date,
    s.first_order_date,
    s.last_order_date,
    COALESCE(s.order_count, 0) AS order_count,
    CAST(COALESCE(s.lifetime_revenue, 0) AS DECIMAL(14, 2)) AS lifetime_revenue,
    CAST(s.lifetime_revenue / s.order_count AS DECIMAL(12, 2)) AS avg_order_value,
    COALESCE(s.order_count, 0) >= 2 AS is_repeat_customer,
    DATEDIFF(s.second_order_date, s.first_order_date) AS days_to_second_order
FROM silver.customers AS c
LEFT JOIN per_customer AS s ON s.customer_id = c.customer_id
