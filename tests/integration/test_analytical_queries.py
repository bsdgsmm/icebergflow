"""Every analytical query runs against real pipeline output and agrees with Gold."""

from datetime import date
from decimal import Decimal

import pytest

from icebergflow.analytics.queries import list_queries, run_query
from icebergflow.datagen import GeneratorConfig, generate
from icebergflow.pipeline import run_pipeline


@pytest.fixture(scope="module")
def lakehouse_with_data(spark, module_lakehouse):
    generate(
        GeneratorConfig(
            days=8,
            orders_per_day=50,
            initial_customers=50,
            products=20,
            seed=13,
            anomaly_rate=0.04,
            schema_v2_from=date(2024, 1, 5),
        ),
        module_lakehouse.raw_dir,
    )
    run_pipeline(spark, module_lakehouse)
    return module_lakehouse


def _rows(spark, settings, name):
    return [r.asDict() for r in run_query(spark, settings.sql_dir, name).collect()]


def test_all_queries_return_rows(spark, lakehouse_with_data):
    for query in list_queries(lakehouse_with_data.sql_dir):
        assert _rows(spark, lakehouse_with_data, query.name), query.name


def test_revenue_trend_moving_average(spark, lakehouse_with_data):
    rows = _rows(spark, lakehouse_with_data, "revenue_trend")
    assert len(rows) == spark.table("gold.daily_sales").count()
    window = [r["net_revenue"] for r in rows[:7]]
    expected = (sum(window) / 7).quantize(Decimal("0.01"))
    assert abs(rows[6]["net_revenue_7d_avg"] - expected) <= Decimal("0.01")
    assert rows[0]["day_over_day_change"] is None


def test_top_products_are_ordered_and_limited(spark, lakehouse_with_data):
    rows = _rows(spark, lakehouse_with_data, "top_products")
    revenues = [r["net_revenue"] for r in rows]
    assert len(rows) == 10
    assert revenues == sorted(revenues, reverse=True)
    best = spark.sql("SELECT MAX(net_revenue) FROM gold.product_performance").first()[0]
    assert revenues[0] == best


def test_shares_sum_to_one(spark, lakehouse_with_data):
    for name in ("channel_mix", "repeat_customer_value"):
        total = sum(r["revenue_share"] for r in _rows(spark, lakehouse_with_data, name))
        assert abs(total - 1) <= Decimal("0.001"), name


def test_channel_mix_includes_pre_v2_orders_as_unknown(spark, lakehouse_with_data):
    channels = {r["channel"] for r in _rows(spark, lakehouse_with_data, "channel_mix")}
    assert "unknown" in channels
    assert channels - {"unknown"} <= {"web", "mobile_app", "marketplace"}


def test_repeat_segments_cover_all_buyers(spark, lakehouse_with_data):
    rows = _rows(spark, lakehouse_with_data, "repeat_customer_value")
    buyers = spark.sql("SELECT COUNT(*) FROM gold.customer_summary WHERE order_count > 0").first()
    assert sum(r["customers"] for r in rows) == buyers[0]
    assert {r["segment"] for r in rows} == {"repeat", "one_time"}


def test_quarantine_report_matches_quarantine_table(spark, lakehouse_with_data):
    rows = _rows(spark, lakehouse_with_data, "quarantine_report")
    reasons = spark.sql("SELECT SUM(size(reasons)) FROM silver.quarantine").first()[0]
    assert sum(r["records"] for r in rows) == reasons
