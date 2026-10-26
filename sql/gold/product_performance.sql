-- gold.product_performance
-- Grain: one row per product_id (every Silver product, including unsold ones).
-- Sales count only order lines of net orders (placed, shipped, delivered).
WITH sold AS (
    SELECT
        i.product_id,
        o.order_id,
        o.order_date,
        i.quantity,
        i.line_amount
    FROM silver.order_items AS i
    INNER JOIN silver.orders AS o ON o.order_id = i.order_id
    WHERE o.status IN ('placed', 'shipped', 'delivered')
),

per_product AS (
    SELECT
        product_id,
        SUM(quantity) AS units_sold,
        COUNT(DISTINCT order_id) AS orders,
        SUM(line_amount) AS net_revenue,
        MIN(order_date) AS first_sold_date,
        MAX(order_date) AS last_sold_date
    FROM sold
    GROUP BY product_id
)

SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.brand,
    p.unit_price AS current_unit_price,
    CAST(COALESCE(s.units_sold, 0) AS BIGINT) AS units_sold,
    COALESCE(s.orders, 0) AS orders,
    CAST(COALESCE(s.net_revenue, 0) AS DECIMAL(14, 2)) AS net_revenue,
    s.first_sold_date,
    s.last_sold_date,
    RANK() OVER (
        PARTITION BY p.category
        ORDER BY COALESCE(s.net_revenue, 0) DESC
    ) AS category_revenue_rank
FROM silver.products AS p
LEFT JOIN per_product AS s ON s.product_id = p.product_id
