-- Question: What share of each monthly cohort placed a second order, and how fast?
SELECT
    cohort_month,
    cohort_customers,
    repeat_customers,
    repeat_rate,
    CAST(repeat_within_7_days / cohort_customers AS DECIMAL(6, 4)) AS repeat_rate_7d,
    CAST(repeat_within_30_days / cohort_customers AS DECIMAL(6, 4)) AS repeat_rate_30d,
    avg_orders_per_customer,
    avg_days_to_second_order
FROM gold.repeat_purchase_cohorts
ORDER BY cohort_month
