"""Silver-to-Gold consistency on generated data: totals must reconcile across layers."""

import pytest

from icebergflow.datagen import GeneratorConfig, generate
from icebergflow.ingestion.bronze import ingest
from icebergflow.transformations.gold import run_gold
from icebergflow.transformations.silver import run_silver

NET = "('placed', 'shipped', 'delivered')"


@pytest.fixture(scope="module")
def built(spark, module_lakehouse):
    generate(
        GeneratorConfig(days=6, orders_per_day=60, initial_customers=50, products=20, seed=5),
        module_lakehouse.raw_dir,
    )
    ingest(spark, module_lakehouse)
    run_silver(spark)
    run_gold(spark, module_lakehouse.sql_dir)
    return spark


def _scalar(spark, query):
    return spark.sql(query).first()[0]


def test_net_revenue_matches_silver(built):
    silver = _scalar(
        built,
        "SELECT SUM(i.line_amount) FROM silver.order_items i "
        f"JOIN silver.orders o ON o.order_id = i.order_id WHERE o.status IN {NET}",
    )
    assert silver > 0
    assert _scalar(built, "SELECT SUM(net_revenue) FROM gold.daily_sales") == silver
    assert _scalar(built, "SELECT SUM(net_revenue) FROM gold.product_performance") == silver
    assert _scalar(built, "SELECT SUM(net_revenue) FROM gold.category_monthly") == silver
    assert _scalar(built, "SELECT SUM(lifetime_revenue) FROM gold.customer_summary") == silver


def test_order_and_unit_counts_reconcile(built):
    assert _scalar(built, "SELECT SUM(orders) FROM gold.daily_sales") == _scalar(
        built, "SELECT COUNT(*) FROM silver.orders"
    )
    units = _scalar(built, "SELECT SUM(units_sold) FROM gold.daily_sales")
    assert units == _scalar(built, "SELECT SUM(units_sold) FROM gold.product_performance")
    assert _scalar(built, "SELECT SUM(net_orders) FROM gold.daily_sales") == _scalar(
        built, "SELECT SUM(order_count) FROM gold.customer_summary"
    )


def test_dimension_coverage(built):
    assert _scalar(built, "SELECT COUNT(*) FROM gold.customer_summary") == _scalar(
        built, "SELECT COUNT(*) FROM silver.customers"
    )
    assert _scalar(built, "SELECT COUNT(*) FROM gold.product_performance") == _scalar(
        built, "SELECT COUNT(*) FROM silver.products"
    )
    buyers = _scalar(built, "SELECT COUNT(*) FROM gold.customer_summary WHERE order_count > 0")
    assert (
        _scalar(built, "SELECT SUM(cohort_customers) FROM gold.repeat_purchase_cohorts") == buyers
    )


def test_category_shares_sum_to_one(built):
    for row in built.sql(
        "SELECT order_month, SUM(revenue_share) AS total FROM gold.category_monthly "
        "GROUP BY order_month"
    ).collect():
        assert abs(float(row.total) - 1.0) < 0.001
