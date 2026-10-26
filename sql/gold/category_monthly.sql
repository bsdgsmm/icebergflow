-- gold.category_monthly
-- Grain: one row per (order_month, category) with net sales in that month.
-- revenue_share is the category's share of that month's net revenue.
WITH sold AS (
    SELECT
        CAST(DATE_TRUNC('MONTH', o.order_date) AS DATE) AS order_month,
        p.category,
        o.order_id,
        i.quantity,
        i.line_amount
    FROM silver.order_items AS i
    INNER JOIN silver.orders AS o ON o.order_id = i.order_id
    INNER JOIN silver.products AS p ON p.product_id = i.product_id
    WHERE o.status IN ('placed', 'shipped', 'delivered')
)

SELECT
    order_month,
    category,
    CAST(SUM(quantity) AS BIGINT) AS units_sold,
    COUNT(DISTINCT order_id) AS orders,
    CAST(SUM(line_amount) AS DECIMAL(14, 2)) AS net_revenue,
    CAST(
        SUM(line_amount) / SUM(SUM(line_amount)) OVER (PARTITION BY order_month)
        AS DECIMAL(6, 4)
    ) AS revenue_share
FROM sold
GROUP BY order_month, category
