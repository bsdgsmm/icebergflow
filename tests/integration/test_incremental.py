"""Incremental Silver/Gold processing driven by Iceberg snapshot ranges and watermarks."""

from datetime import date

import pytest

from icebergflow.datagen import GeneratorConfig, generate
from icebergflow.ingestion.bronze import ingest
from icebergflow.lakehouse.catalog import current_snapshot_id, snapshots
from icebergflow.lakehouse.watermarks import get_watermark
from icebergflow.pipeline import run_pipeline
from icebergflow.transformations import silver as silver_module
from icebergflow.transformations.gold import run_gold
from icebergflow.transformations.silver import run_silver

CONFIG = GeneratorConfig(
    start_date=date(2024, 1, 29),
    days=5,
    orders_per_day=40,
    initial_customers=40,
    daily_new_customers=3,
    products=16,
    seed=21,
)
BATCHES = ["2024-01-29", "2024-01-30", "2024-01-31", "2024-02-01", "2024-02-02"]
TABLES = [
    "silver.customers",
    "silver.products",
    "silver.orders",
    "silver.order_items",
    "gold.daily_sales",
    "gold.product_performance",
    "gold.customer_summary",
]


@pytest.fixture
def raw(lakehouse):
    generate(CONFIG, lakehouse.raw_dir)
    return lakehouse


def _content(spark, table):
    df = spark.table(table)
    return sorted(
        tuple(r) for r in df.drop(*[c for c in df.columns if c.startswith("_")]).collect()
    )


def _modes(results):
    return {r.entity: r.mode for r in results}


def test_first_run_is_full_and_sets_watermarks(spark, raw):
    ingest(spark, raw, BATCHES[:1])
    results = run_silver(spark)

    assert set(_modes(results).values()) == {"full"}
    for entity in ("customers", "orders", "order_items"):
        bronze_snapshot = current_snapshot_id(spark, f"bronze.{entity}")
        assert get_watermark(spark, f"silver.{entity}", f"bronze.{entity}") == bronze_snapshot


def test_next_run_reads_only_the_new_batch(spark, raw):
    ingest(spark, raw, BATCHES[:1])
    run_silver(spark)
    ingest(spark, raw, BATCHES[1:2])

    results = run_silver(spark)

    assert set(_modes(results).values()) == {"incremental"}
    for result in results:
        new_rows = spark.table(f"bronze.{result.entity}").where(f"_batch_id = '{BATCHES[1]}'")
        assert result.input_rows == new_rows.count()


def test_run_without_new_data_commits_nothing(spark, raw):
    ingest(spark, raw, BATCHES[:2])
    run_silver(spark)
    before = {t: len(snapshots(spark, t)) for t in TABLES[:4]}

    results = run_silver(spark)

    assert set(_modes(results).values()) == {"up_to_date"}
    assert {t: len(snapshots(spark, t)) for t in TABLES[:4]} == before


def test_incremental_result_equals_full_refresh(spark, raw):
    for batch in BATCHES:
        run_pipeline(spark, raw, [batch])
    incremental = {t: _content(spark, t) for t in TABLES}
    quarantined = spark.table("silver.quarantine").count()

    result = run_pipeline(spark, raw, full_refresh=True)

    assert set(_modes(result.silver).values()) == {"full"}
    assert {t: _content(spark, t) for t in TABLES} == incremental
    assert spark.table("silver.quarantine").count() == quarantined


def test_crash_before_watermark_update_is_recovered(spark, raw, monkeypatch):
    ingest(spark, raw, BATCHES[:1])
    run_silver(spark)
    ingest(spark, raw, BATCHES[1:3])

    real = silver_module.set_watermark

    def failing(spark_, consumer, source, snapshot_id):
        if consumer == "silver.orders":
            raise RuntimeError("simulated crash after merge")
        real(spark_, consumer, source, snapshot_id)

    monkeypatch.setattr(silver_module, "set_watermark", failing)
    with pytest.raises(RuntimeError, match="simulated crash"):
        run_silver(spark)
    monkeypatch.setattr(silver_module, "set_watermark", real)

    results = _modes(run_silver(spark))
    assert results["orders"] == "incremental", "the same range is re-read"
    recovered = {t: _content(spark, t) for t in TABLES[:4]}
    run_silver(spark, full_refresh=True)
    assert {t: _content(spark, t) for t in TABLES[:4]} == recovered


def test_bronze_rollback_triggers_full_read(spark, settings, raw):
    ingest(spark, raw, BATCHES[:1])
    first = current_snapshot_id(spark, "bronze.orders")
    ingest(spark, raw, BATCHES[1:2])
    run_silver(spark)
    spark.sql(f"CALL {settings.catalog}.system.rollback_to_snapshot('bronze.orders', {first})")
    ingest(spark, raw, BATCHES[1:3])

    modes = _modes(run_silver(spark))

    assert modes["orders"] == "full"
    assert modes["customers"] == "incremental"


def test_gold_refresh_rewrites_only_affected_month(spark, raw):
    for batch in BATCHES:
        run_pipeline(spark, raw, [batch])
    january = "partition.order_date_month = 648"
    jan_files = {
        r.file_path
        for r in spark.sql(
            f"SELECT file_path FROM gold.daily_sales.files WHERE {january}"
        ).collect()
    }
    jan_rows = _content(spark, "gold.daily_sales")
    commits = len(snapshots(spark, "gold.daily_sales"))

    [unchanged] = run_gold(spark, raw.sql_dir, ["daily_sales"], months=[])
    [refreshed] = run_gold(spark, raw.sql_dir, ["daily_sales"], months=[date(2024, 2, 1)])

    assert unchanged.mode == "unchanged"
    assert refreshed.mode == "partitions"
    history = snapshots(spark, "gold.daily_sales")
    assert len(history) == commits + 1
    assert history[-1].summary["changed-partition-count"] == "1"
    assert {
        r.file_path
        for r in spark.sql(
            f"SELECT file_path FROM gold.daily_sales.files WHERE {january}"
        ).collect()
    } == jan_files
    assert _content(spark, "gold.daily_sales") == jan_rows
