import json

import pytest

from icebergflow.datagen import GeneratorConfig, generate
from icebergflow.ingestion.bronze import (
    IngestionError,
    SchemaDriftError,
    committed_batches,
    ingest,
)
from icebergflow.lakehouse.catalog import snapshots

CONFIG = GeneratorConfig(
    days=3, orders_per_day=30, initial_customers=40, daily_new_customers=4, products=16, seed=3
)


@pytest.fixture
def raw(lakehouse):
    generate(CONFIG, lakehouse.raw_dir)
    return lakehouse


def _manifest(settings, batch_id):
    return json.loads((settings.raw_dir / batch_id / "_manifest.json").read_text())


def test_bronze_row_counts_match_source_manifests(spark, raw):
    results = ingest(spark, raw)

    assert {r.status for r in results} == {"ingested"}
    for batch_id in ("2024-01-01", "2024-01-02", "2024-01-03"):
        expected = _manifest(raw, batch_id)["row_counts"]
        for entity, count in expected.items():
            actual = spark.table(f"bronze.{entity}").where(f"_batch_id = '{batch_id}'").count()
            assert actual == count, (entity, batch_id)


def test_bronze_keeps_raw_strings_and_lineage(spark, raw):
    ingest(spark, raw, ["2024-01-01"])

    schema = {f.name: f.dataType.simpleString() for f in spark.table("bronze.orders").schema}
    assert schema["order_ts"] == "string"
    assert schema["_ingested_at"] == "timestamp"
    row = spark.table("bronze.order_items").first()
    assert row._batch_id == "2024-01-01"
    assert row._source_file == "2024-01-01/order_items.jsonl"
    assert len(row._record_hash) == 64
    assert row._corrupt_record is None


def test_rerun_is_a_no_op(spark, raw):
    ingest(spark, raw)
    counts = {e: spark.table(f"bronze.{e}").count() for e in ("orders", "order_items")}
    snapshot_ids = [s.snapshot_id for s in snapshots(spark, "bronze.orders")]

    results = ingest(spark, raw)

    assert {r.status for r in results} == {"skipped"}
    assert {e: spark.table(f"bronze.{e}").count() for e in counts} == counts
    assert [s.snapshot_id for s in snapshots(spark, "bronze.orders")] == snapshot_ids


def test_batch_marker_is_committed_with_the_data(spark, raw):
    ingest(spark, raw, ["2024-01-01", "2024-01-02"])

    history = snapshots(spark, "bronze.orders")
    assert [s.summary["icebergflow.batch-id"] for s in history] == ["2024-01-01", "2024-01-02"]
    assert committed_batches(spark, "bronze.orders") == {"2024-01-01", "2024-01-02"}


def test_resume_after_partial_failure(spark, raw):
    ingest(spark, raw, entities=["customers", "products"])
    (raw.raw_dir / "2024-01-02" / "orders.jsonl").rename(raw.raw_dir / "orders.tmp")

    with pytest.raises(IngestionError, match="missing source file"):
        ingest(spark, raw)
    assert committed_batches(spark, "bronze.orders") == {"2024-01-01"}

    (raw.raw_dir / "orders.tmp").rename(raw.raw_dir / "2024-01-02" / "orders.jsonl")
    results = ingest(spark, raw)

    by_entity = {(r.entity, r.batch_id): r.status for r in results}
    assert by_entity[("customers", "2024-01-01")] == "skipped"
    assert by_entity[("orders", "2024-01-01")] == "skipped"
    assert by_entity[("orders", "2024-01-02")] == "ingested"
    total = sum(
        _manifest(raw, b)["row_counts"]["orders"]
        for b in ("2024-01-01", "2024-01-02", "2024-01-03")
    )
    assert spark.table("bronze.orders").count() == total


def test_rolled_back_batch_is_reloaded(spark, settings, raw):
    ingest(spark, raw, ["2024-01-01"], ["orders"])
    first = snapshots(spark, "bronze.orders")[0].snapshot_id
    ingest(spark, raw, ["2024-01-02"], ["orders"])
    spark.sql(f"CALL {settings.catalog}.system.rollback_to_snapshot('bronze.orders', {first})")

    results = ingest(spark, raw, ["2024-01-01", "2024-01-02"], ["orders"])

    assert [r.status for r in results] == ["skipped", "ingested"]
    assert committed_batches(spark, "bronze.orders") == {"2024-01-01", "2024-01-02"}


def test_row_count_mismatch_rejects_batch_without_commit(spark, raw):
    path = raw.raw_dir / "2024-01-01" / "order_items.jsonl"
    lines = path.read_text().splitlines()
    path.write_text("\n".join(lines[:-2]) + "\n")

    with pytest.raises(IngestionError, match="manifest declares"):
        ingest(spark, raw, ["2024-01-01"], ["order_items"])
    assert snapshots(spark, "bronze.order_items") == []


def test_malformed_json_lines_are_kept_as_corrupt_records(spark, raw):
    path = raw.raw_dir / "2024-01-01" / "orders.jsonl"
    path.write_text(path.read_text() + '{"order_id": "ORD-BROKEN", \n')
    manifest = _manifest(raw, "2024-01-01")
    manifest["row_counts"]["orders"] += 1
    (raw.raw_dir / "2024-01-01" / "_manifest.json").write_text(json.dumps(manifest))

    ingest(spark, raw, ["2024-01-01"], ["orders"])

    corrupt = spark.table("bronze.orders").where("_corrupt_record IS NOT NULL").collect()
    assert len(corrupt) == 1
    assert "ORD-BROKEN" in corrupt[0]._corrupt_record


def test_invalid_column_names_are_rejected(spark, raw):
    path = raw.raw_dir / "2024-01-01" / "orders.jsonl"
    lines = path.read_text().splitlines()
    lines[0] = lines[0][:-1] + ', "Order Note": "fragile"}'
    path.write_text("\n".join(lines) + "\n")

    with pytest.raises(SchemaDriftError, match="Order Note"):
        ingest(spark, raw, ["2024-01-01"], ["orders"])
