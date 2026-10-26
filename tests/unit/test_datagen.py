import csv
import hashlib
import json
from collections import Counter
from datetime import date

import pytest

from icebergflow.datagen import (
    ORDER_COLUMNS_V1,
    ORDER_COLUMNS_V2,
    ORDER_STATUSES,
    UNKNOWN_CUSTOMER_ID,
    GeneratorConfig,
    generate,
)

SMALL = GeneratorConfig(
    days=6,
    orders_per_day=60,
    initial_customers=80,
    daily_new_customers=5,
    products=24,
    seed=7,
    schema_v2_from=date(2024, 1, 4),
    anomaly_rate=0.05,
)


def _digest(root):
    sha = hashlib.sha256()
    for path in sorted(p for p in root.rglob("*") if p.is_file()):
        sha.update(path.relative_to(root).as_posix().encode())
        sha.update(path.read_bytes())
    return sha.hexdigest()


def _jsonl(path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


@pytest.fixture(scope="module")
def batches(tmp_path_factory):
    root = tmp_path_factory.mktemp("raw")
    return root, generate(SMALL, root)


def test_output_is_byte_for_byte_reproducible(batches, tmp_path):
    root, _ = batches
    generate(SMALL, tmp_path)
    assert _digest(root) == _digest(tmp_path)


def test_different_seed_changes_output(batches, tmp_path):
    root, _ = batches
    generate(GeneratorConfig(**{**SMALL.__dict__, "seed": 8}), tmp_path)
    assert _digest(root) != _digest(tmp_path)


def test_one_directory_per_day_with_expected_files(batches):
    root, summaries = batches
    assert [s.batch_id for s in summaries] == [f"2024-01-0{d}" for d in range(1, 7)]
    for summary in summaries:
        names = {p.name for p in (root / summary.batch_id).iterdir()}
        assert names == {
            "customers.csv",
            "products.csv",
            "orders.jsonl",
            "order_items.jsonl",
            "_manifest.json",
        }


def test_manifest_row_counts_match_files(batches):
    root, summaries = batches
    for summary in summaries:
        batch = root / summary.batch_id
        manifest = json.loads((batch / "_manifest.json").read_text(encoding="utf-8"))
        assert manifest["row_counts"]["orders"] == len(_jsonl(batch / "orders.jsonl"))
        assert manifest["row_counts"]["order_items"] == len(_jsonl(batch / "order_items.jsonl"))
        with (batch / "customers.csv").open(encoding="utf-8") as handle:
            assert manifest["row_counts"]["customers"] == len(list(csv.DictReader(handle)))


def test_schema_version_switches_on_configured_date(batches):
    root, summaries = batches
    for summary in summaries:
        first = _jsonl(root / summary.batch_id / "orders.jsonl")[0]
        expected = ORDER_COLUMNS_V2 if summary.batch_id >= "2024-01-04" else ORDER_COLUMNS_V1
        assert list(first) == expected
        assert summary.schema_version == (2 if summary.batch_id >= "2024-01-04" else 1)


def test_first_batch_contains_full_reference_data(batches):
    root, _ = batches
    with (root / "2024-01-01" / "products.csv").open(encoding="utf-8") as handle:
        products = list(csv.DictReader(handle))
    assert len(products) == SMALL.products
    assert len({p["product_id"] for p in products}) == SMALL.products


def test_injected_anomalies_are_present_in_data(batches):
    root, summaries = batches
    totals = Counter()
    for summary in summaries:
        totals.update(summary.injected_anomalies)
    assert sum(totals.values()) > 0

    all_orders = [o for s in summaries for o in _jsonl(root / s.batch_id / "orders.jsonl")]
    statuses = Counter(o["status"] for o in all_orders)
    assert statuses["unknown"] == totals["orders.invalid_status"]
    missing = sum(1 for o in all_orders if o["customer_id"] is None)
    assert missing == totals["orders.missing_customer_id"]
    unknown = sum(1 for o in all_orders if o["customer_id"] == UNKNOWN_CUSTOMER_ID)
    assert unknown == totals["orders.unknown_customer"]


def test_orders_have_lifecycle_updates_and_valid_statuses(batches):
    root, summaries = batches
    events = [o for s in summaries for o in _jsonl(root / s.batch_id / "orders.jsonl")]
    valid = [o for o in events if o["status"] in ORDER_STATUSES]
    per_order = Counter(o["order_id"] for o in valid)
    assert max(per_order.values()) > 1, "expected some orders to change status over time"
    assert {o["status"] for o in valid} >= {"placed", "shipped", "delivered"}


def test_duplicates_are_reported(batches):
    _, summaries = batches
    assert sum(s.duplicates["orders"] + s.duplicates["order_items"] for s in summaries) > 0


@pytest.mark.parametrize(
    "overrides",
    [{"days": 0}, {"orders_per_day": 0}, {"anomaly_rate": 0.7}, {"daily_new_customers": -1}],
)
def test_invalid_configuration_rejected(overrides):
    with pytest.raises(ValueError):
        GeneratorConfig(**overrides)
