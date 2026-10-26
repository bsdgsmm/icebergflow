"""End-to-end Bronze-to-Silver tests on generated data with known injected defects."""

import json
from collections import Counter

import pytest

from icebergflow.datagen import GeneratorConfig, generate
from icebergflow.ingestion.bronze import ingest
from icebergflow.transformations.silver import run_silver

CONFIG = GeneratorConfig(
    days=5,
    orders_per_day=50,
    initial_customers=60,
    daily_new_customers=5,
    products=20,
    seed=11,
    anomaly_rate=0.05,
)

ANOMALY_REASONS = {
    "orders.missing_customer_id": ("orders", "missing_customer_id"),
    "orders.unknown_customer": ("orders", "unknown_customer"),
    "orders.malformed_order_ts": ("orders", "invalid_order_ts"),
    "orders.invalid_status": ("orders", "invalid_status"),
    "order_items.negative_quantity": ("order_items", "non_positive_quantity"),
    "order_items.unknown_product": ("order_items", "unknown_product"),
    "order_items.null_price": ("order_items", "missing_unit_price"),
    "customers.malformed_email": ("customers", "malformed_email"),
    "products.negative_price": ("products", "negative_unit_price"),
}


def _jsonl(path):
    return [json.loads(line) for line in path.read_text().splitlines()]


@pytest.fixture(scope="module")
def processed(spark, module_lakehouse):
    lakehouse = module_lakehouse
    summaries = generate(CONFIG, lakehouse.raw_dir)
    ingest(spark, lakehouse)
    results = {r.entity: r for r in run_silver(spark)}
    return lakehouse, summaries, results


def _table_rows(spark, table):
    return sorted(tuple(r) for r in spark.table(table).drop("_processed_at").collect())


def test_business_keys_are_unique(spark, processed):
    for table, keys in {
        "silver.customers": "customer_id",
        "silver.products": "product_id",
        "silver.orders": "order_id",
        "silver.order_items": "order_id, line_number",
    }.items():
        dupes = spark.sql(
            f"SELECT {keys}, count(*) c FROM {table} GROUP BY {keys} HAVING c > 1"
        ).count()
        assert dupes == 0, table


def test_every_injected_defect_is_quarantined_with_its_reason(spark, processed):
    _, summaries, _ = processed
    injected = Counter()
    for summary in summaries:
        injected.update(summary.injected_anomalies)
    assert sum(injected.values()) > 10

    rows = spark.sql(
        "SELECT entity, reason, count(*) AS n FROM silver.quarantine "
        "LATERAL VIEW explode(reasons) r AS reason GROUP BY entity, reason"
    ).collect()
    quarantined = {(r.entity, r.reason): r.n for r in rows}
    for anomaly, key in ANOMALY_REASONS.items():
        assert quarantined.get(key, 0) == injected[anomaly], anomaly


def test_items_of_rejected_orders_are_quarantined_as_orphans(spark, processed):
    settings, summaries, _ = processed
    rejected = {
        r.record_json_order_id
        for r in spark.sql(
            "SELECT get_json_object(record, '$.order_id') AS record_json_order_id "
            "FROM silver.quarantine WHERE entity = 'orders'"
        ).collect()
    }
    orphan_hashes = set()
    for summary in summaries:
        for item in _jsonl(settings.raw_dir / summary.batch_id / "order_items.jsonl"):
            if item["order_id"] in rejected:
                orphan_hashes.add((summary.batch_id, json.dumps(item, sort_keys=True)))
    orphans = (
        spark.sql(
            "SELECT count(*) AS n FROM silver.quarantine "
            "WHERE entity = 'order_items' AND array_contains(reasons, 'unknown_order')"
        )
        .first()
        .n
    )
    assert orphans == len(orphan_hashes) > 0


def test_order_status_reflects_latest_event(spark, processed):
    settings, summaries, _ = processed
    latest = {}
    for summary in summaries:
        for event in _jsonl(settings.raw_dir / summary.batch_id / "orders.jsonl"):
            current = latest.get(event["order_id"])
            if current is None or event["updated_at"] > current["updated_at"]:
                latest[event["order_id"]] = event

    silver = {r.order_id: r.status for r in spark.table("silver.orders").collect()}
    assert silver
    for order_id, status in silver.items():
        assert latest[order_id]["status"] == status, order_id
    assert Counter(silver.values())["delivered"] > 0


def test_bronze_to_silver_reconciliation(spark, processed):
    _, _, results = processed
    for entity, result in results.items():
        assert result.input_rows == spark.table(f"bronze.{entity}").count()
        assert result.input_rows == result.valid_rows + result.quarantined_rows
        assert spark.table(f"silver.{entity}").count() <= result.merged_rows
    assert results["orders"].duplicates_removed > 0


def test_reprocessing_is_idempotent(spark, processed):
    before = {t: _table_rows(spark, t) for t in ("silver.orders", "silver.order_items")}
    quarantine_before = spark.table("silver.quarantine").count()

    run_silver(spark, full_refresh=True)

    assert {t: _table_rows(spark, t) for t in before} == before
    assert spark.table("silver.quarantine").count() == quarantine_before


def test_typed_schema(spark, processed):
    types = {f.name: f.dataType.simpleString() for f in spark.table("silver.order_items").schema}
    assert types["quantity"] == "int"
    assert types["unit_price"] == "decimal(10,2)"
    assert types["line_amount"] == "decimal(12,2)"
    order_types = {f.name: f.dataType.simpleString() for f in spark.table("silver.orders").schema}
    assert order_types["order_ts"] == "timestamp"
    assert order_types["order_date"] == "date"
