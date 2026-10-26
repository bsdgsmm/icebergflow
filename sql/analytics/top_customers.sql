-- Question: Who are the ten most valuable customers and how quickly did they come back?
SELECT
    customer_id,
    country,
    first_order_date,
    last_order_date,
    order_count,
    lifetime_revenue,
    avg_order_value,
    days_to_second_order
FROM gold.customer_summary
WHERE order_count > 0
ORDER BY lifetime_revenue DESC, customer_id
LIMIT 10
