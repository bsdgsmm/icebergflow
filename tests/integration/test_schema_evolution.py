"""Schema and partition evolution across Bronze, Silver and Gold."""

from datetime import date

import pytest

from icebergflow.datagen import GeneratorConfig, generate
from icebergflow.lakehouse.catalog import (
    add_partition_field,
    partition_fields,
    snapshots,
)
from icebergflow.lakehouse.metrics import collect_with_scan_metrics
from icebergflow.pipeline import run_pipeline
from icebergflow.transformations.silver import ensure_silver_tables

CONFIG = GeneratorConfig(
    days=4,
    orders_per_day=40,
    initial_customers=40,
    daily_new_customers=3,
    products=16,
    seed=31,
    schema_v2_from=date(2024, 1, 3),
)
V1 = ["2024-01-01", "2024-01-02"]
V2 = ["2024-01-03", "2024-01-04"]


@pytest.fixture
def raw(lakehouse):
    generate(CONFIG, lakehouse.raw_dir)
    return lakehouse


def _schema_ids(spark, table):
    return [
        r.latest_schema_id
        for r in spark.sql(
            f"SELECT latest_schema_id FROM {table}.metadata_log_entries "
            "WHERE latest_schema_id IS NOT NULL ORDER BY timestamp"
        ).collect()
    ]


def test_bronze_adds_new_source_columns(spark, raw):
    run_pipeline(spark, raw, V1)
    v1_snapshot = snapshots(spark, "bronze.orders")[-1].snapshot_id
    assert "channel" not in spark.table("bronze.orders").columns

    run_pipeline(spark, raw, V2)

    columns = spark.table("bronze.orders").columns
    assert columns.index("channel") == columns.index("updated_at") + 1
    assert columns.index("coupon_code") == columns.index("channel") + 1
    assert max(_schema_ids(spark, "bronze.orders")) > min(_schema_ids(spark, "bronze.orders"))

    old_rows = spark.table("bronze.orders").where(f"_batch_id IN ('{V1[0]}', '{V1[1]}')")
    assert old_rows.where("channel IS NOT NULL").count() == 0
    new_rows = spark.table("bronze.orders").where(f"_batch_id = '{V2[0]}'")
    assert new_rows.where("channel IS NOT NULL").count() > 0

    historical = spark.sql(f"SELECT * FROM bronze.orders VERSION AS OF {v1_snapshot}")
    assert "channel" not in historical.columns, "time travel uses the snapshot's schema"
    assert historical.count() == old_rows.count()
    # The DataFrame reader option selects the snapshot's files but projects the
    # current table schema, so the newer columns appear as NULL.
    by_option = spark.read.option("snapshot-id", str(v1_snapshot)).table("bronze.orders")
    assert "channel" in by_option.columns
    assert by_option.where("channel IS NOT NULL").count() == 0


def test_silver_and_gold_expose_v2_attributes(spark, raw):
    run_pipeline(spark, raw, V1 + V2)

    orders = spark.table("silver.orders")
    v1_orders = orders.where("order_date < date'2024-01-03'")
    v2_orders = orders.where("order_date >= date'2024-01-03'")
    assert v1_orders.where("channel IS NOT NULL").count() == 0
    assert v2_orders.where("channel IS NULL").count() == 0
    assert {r.channel for r in v2_orders.select("channel").distinct().collect()} <= {
        "web",
        "mobile_app",
        "marketplace",
    }
    assert v2_orders.where("coupon_code IS NOT NULL").count() > 0

    channels = {
        r.channel: r.net_orders
        for r in spark.sql(
            "SELECT channel, SUM(net_orders) AS net_orders FROM gold.channel_daily GROUP BY channel"
        ).collect()
    }
    assert channels["unknown"] > 0
    assert (
        sum(channels.values())
        == spark.sql("SELECT SUM(net_orders) FROM gold.daily_sales").first()[0]
    )


def test_existing_silver_table_is_evolved_in_place(spark, lakehouse):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS silver")
    spark.sql(
        "CREATE TABLE silver.orders (order_id STRING, customer_id STRING, order_ts TIMESTAMP, "
        "order_date DATE, status STRING, currency STRING, updated_at TIMESTAMP, "
        "_batch_id STRING, _processed_at TIMESTAMP) USING iceberg PARTITIONED BY (days(order_ts))"
    )
    spark.sql(
        "INSERT INTO silver.orders VALUES ('O1', 'C1', timestamp'2024-01-01 10:00:00', "
        "date'2024-01-01', 'placed', 'USD', timestamp'2024-01-01 10:00:00', 'b1', "
        "current_timestamp())"
    )
    files_before = spark.sql("SELECT file_path FROM silver.orders.files").collect()

    ensure_silver_tables(spark)

    columns = spark.table("silver.orders").columns
    assert columns[columns.index("updated_at") + 1 : columns.index("_batch_id")] == [
        "channel",
        "coupon_code",
    ]
    assert spark.table("silver.orders").first().channel is None
    assert spark.sql("SELECT file_path FROM silver.orders.files").collect() == files_before


def test_partition_evolution_keeps_old_files_and_prunes_new_ones(spark, raw):
    run_pipeline(spark, raw, V1)
    table = "silver.order_items"
    assert partition_fields(spark, table) == []
    rows_before = spark.table(table).count()

    assert add_partition_field(spark, table, "bucket(8, order_id)") is True
    assert add_partition_field(spark, table, "bucket(8, order_id)") is False
    run_pipeline(spark, raw, V2)

    assert partition_fields(spark, table) == ["bucket(8, order_id)"]
    specs = {
        r.spec_id: r.n
        for r in spark.sql(
            f"SELECT spec_id, count(*) AS n FROM {table}.files GROUP BY spec_id"
        ).collect()
    }
    assert set(specs) == {0, 1}, "files written before and after the spec change coexist"
    assert spark.table(table).count() > rows_before

    new_order = (
        spark.sql(f"SELECT order_id FROM {table} WHERE _batch_id = '{V2[1]}' LIMIT 1")
        .first()
        .order_id
    )
    rows, metrics = collect_with_scan_metrics(spark.table(table).where(f"order_id = '{new_order}'"))
    assert rows
    assert metrics["skippedDataFiles"] > 0
