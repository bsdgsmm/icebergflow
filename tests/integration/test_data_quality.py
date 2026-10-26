"""Quality suites against real pipeline output, and the gate that protects Gold."""

import pytest

from icebergflow.datagen import GeneratorConfig, generate
from icebergflow.lakehouse.catalog import snapshots
from icebergflow.pipeline import run_pipeline
from icebergflow.quality.checks import expression, unique
from icebergflow.quality.suite import DataQualityError, run_checks, run_suite

CONFIG = GeneratorConfig(
    days=3, orders_per_day=40, initial_customers=40, daily_new_customers=3, products=16, seed=8
)
BATCHES = ["2024-01-01", "2024-01-02", "2024-01-03"]


@pytest.fixture
def pipeline(spark, lakehouse):
    generate(CONFIG, lakehouse.raw_dir)
    result = run_pipeline(spark, lakehouse, BATCHES[:2])
    return lakehouse, result


def test_clean_pipeline_passes_every_check(spark, pipeline):
    _, result = pipeline
    assert result.quality
    failed = [r.check for r in result.quality if not r.passed]
    assert failed == []
    suites = {r.suite for r in spark.table("ops.dq_results").select("suite").collect()}
    assert suites == {"silver", "gold"}


def test_results_are_recorded_per_run(spark, pipeline):
    before = spark.table("ops.dq_results").count()
    results = run_suite(spark, "silver")
    rows = spark.table("ops.dq_results").count()
    assert rows == before + len(results)
    latest = spark.sql(
        "SELECT COUNT(DISTINCT run_id) AS runs FROM ops.dq_results WHERE suite = 'silver'"
    ).first()
    assert latest.runs == 2


def test_violations_are_counted_and_raise(spark, pipeline):
    spark.sql(
        "INSERT INTO silver.orders SELECT order_id, customer_id, order_ts, order_date, "
        "'lost', currency, updated_at, channel, coupon_code, _batch_id, _processed_at "
        "FROM silver.orders LIMIT 3"
    )

    with pytest.raises(DataQualityError) as raised:
        run_suite(spark, "silver")

    failures = {f.check: f.failures for f in raised.value.failures}
    assert failures["unique:silver.orders(order_id)"] == 3
    assert failures["accepted_values:silver.orders.status"] == 3


def test_warnings_do_not_raise(spark, pipeline):
    checks = [
        expression("silver.order_items", "quantity < 2", "small_quantities", severity="warn"),
        unique("silver.orders", ["order_id"]),
    ]
    results = run_checks(spark, checks, "adhoc")
    assert results[0].failures > 0
    assert results[1].passed


def test_failed_silver_gate_leaves_gold_untouched(spark, pipeline):
    settings, _ = pipeline
    gold_commits = len(snapshots(spark, "gold.daily_sales"))
    spark.sql("UPDATE silver.order_items SET quantity = 0 WHERE line_number = 1")

    with pytest.raises(DataQualityError, match="positive_quantity"):
        run_pipeline(spark, settings, BATCHES[2:])

    assert len(snapshots(spark, "gold.daily_sales")) == gold_commits
