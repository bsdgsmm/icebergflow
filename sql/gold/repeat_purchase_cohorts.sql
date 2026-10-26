-- gold.repeat_purchase_cohorts
-- Grain: one row per cohort_month, the month of a customer's first net order.
-- Built from gold.customer_summary, so it must run after that model.
SELECT
    CAST(DATE_TRUNC('MONTH', first_order_date) AS DATE) AS cohort_month,
    COUNT(*) AS cohort_customers,
    COUNT_IF(is_repeat_customer) AS repeat_customers,
    CAST(COUNT_IF(is_repeat_customer) / COUNT(*) AS DECIMAL(6, 4)) AS repeat_rate,
    COUNT_IF(days_to_second_order <= 7) AS repeat_within_7_days,
    COUNT_IF(days_to_second_order <= 30) AS repeat_within_30_days,
    CAST(AVG(order_count) AS DECIMAL(8, 2)) AS avg_orders_per_customer,
    CAST(AVG(days_to_second_order) AS DECIMAL(8, 2)) AS avg_days_to_second_order
FROM gold.customer_summary
WHERE first_order_date IS NOT NULL
GROUP BY CAST(DATE_TRUNC('MONTH', first_order_date) AS DATE)
