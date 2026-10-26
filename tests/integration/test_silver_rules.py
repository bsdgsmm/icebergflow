"""Rule-level tests: hand-built Bronze rows through parse + validate + deduplicate."""

import pytest

from icebergflow.ingestion.bronze import ensure_bronze_table
from icebergflow.ingestion.sources import ENTITIES
from icebergflow.transformations.silver import (
    deduplicate,
    ensure_silver_tables,
    parse,
    validate,
)
from icebergflow.transformations.silver_specs import SPECS

GOOD_TS = "2024-01-01T10:00:00Z"


def _bronze(spark, entity, rows):
    columns = list(ENTITIES[entity].columns)
    data = [
        (*[row.get(c) for c in columns], "b1", f"h{i}", row.get("_corrupt_record"))
        for i, row in enumerate(rows)
    ]
    schema = ", ".join(f"`{c}` string" for c in columns)
    return spark.createDataFrame(
        data, f"{schema}, _batch_id string, _record_hash string, _corrupt_record string"
    )


def _reasons(spark, entity, rows):
    spec = SPECS[entity]
    checked = validate(spark, parse(_bronze(spark, entity, rows), spec), spec)
    return [sorted(r._reasons) for r in checked.orderBy("_record_hash").collect()]


@pytest.fixture
def silver_tables(spark, lakehouse):
    for entity in ENTITIES.values():
        ensure_bronze_table(spark, entity)
    ensure_silver_tables(spark)
    spark.sql(
        "INSERT INTO silver.customers VALUES "
        "('CUS-000001', 'Ava', 'Kaya', 'a@example.com', 'TR', date'2024-01-01', "
        "timestamp'2024-01-01 00:00:00', 'b0', current_timestamp())"
    )
    spark.sql(
        "INSERT INTO silver.products VALUES ('PRD-0001', 'Mug', 'Home', 'Contoso', 9.50, true, "
        "timestamp'2024-01-01 00:00:00', 'b0', current_timestamp())"
    )
    spark.sql(
        "INSERT INTO silver.orders VALUES ('ORD-1', 'CUS-000001', timestamp'2024-01-01 10:00:00', "
        "date'2024-01-01', 'placed', 'USD', timestamp'2024-01-01 10:00:00', NULL, NULL, "
        "'b0', current_timestamp())"
    )


def _order(**overrides):
    base = {
        "order_id": "ORD-2",
        "customer_id": "CUS-000001",
        "order_ts": GOOD_TS,
        "status": "placed",
        "currency": "USD",
        "updated_at": GOOD_TS,
    }
    return {**base, **overrides}


def test_order_rules(spark, silver_tables):
    reasons = _reasons(
        spark,
        "orders",
        [
            _order(),
            _order(customer_id=None),
            _order(customer_id="CUS-999999"),
            _order(order_ts="2024/01/01 10:00:00Z"),
            _order(status="unknown"),
            _order(order_id=None, currency="DOLLARS"),
            _order(_corrupt_record='{"order_id": broken'),
        ],
    )
    assert reasons == [
        [],
        ["missing_customer_id"],
        ["unknown_customer"],
        ["invalid_order_ts"],
        ["invalid_status"],
        ["invalid_currency", "missing_order_id"],
        ["corrupt_record"],
    ]


def _item(**overrides):
    base = {
        "order_id": "ORD-1",
        "line_number": "1",
        "product_id": "PRD-0001",
        "quantity": "2",
        "unit_price": "9.5",
        "discount": "0.1",
    }
    return {**base, **overrides}


def test_order_item_rules_and_amount(spark, silver_tables):
    spec = SPECS["order_items"]
    rows = [
        _item(),
        _item(quantity="-1"),
        _item(product_id="PRD-9999"),
        _item(unit_price=None),
        _item(order_id="ORD-404"),
        _item(quantity="two", discount="1.5"),
    ]
    checked = validate(spark, parse(_bronze(spark, "order_items", rows), spec), spec)
    result = checked.orderBy("_record_hash").collect()

    assert [sorted(r._reasons) for r in result] == [
        [],
        ["non_positive_quantity"],
        ["unknown_product"],
        ["missing_unit_price"],
        ["unknown_order"],
        ["invalid_discount", "invalid_quantity"],
    ]
    assert str(result[0].line_amount) == "17.10"


def test_customer_and_product_rules(spark, silver_tables):
    customer = {
        "customer_id": "CUS-000002",
        "first_name": "Leon",
        "last_name": "Rossi",
        "email": "leon@example.com",
        "country": "de",
        "signup_date": "2024-01-02",
        "updated_at": GOOD_TS,
    }
    assert _reasons(
        spark,
        "customers",
        [customer, {**customer, "email": "leon_at_example.com"}, {**customer, "signup_date": "x"}],
    ) == [[], ["malformed_email"], ["invalid_signup_date"]]

    product = {
        "product_id": "PRD-0002",
        "product_name": "Lamp",
        "category": "Home",
        "brand": "Litware",
        "unit_price": "12.00",
        "is_active": "true",
        "updated_at": GOOD_TS,
    }
    assert _reasons(
        spark,
        "products",
        [product, {**product, "unit_price": "-12.00"}, {**product, "is_active": "maybe"}],
    ) == [[], ["negative_unit_price"], ["invalid_is_active"]]


def test_deduplicate_keeps_latest_version(spark, silver_tables):
    spec = SPECS["orders"]
    rows = [
        _order(status="placed", updated_at="2024-01-01T10:00:00Z"),
        _order(status="delivered", updated_at="2024-01-04T10:00:00Z"),
        _order(status="shipped", updated_at="2024-01-02T10:00:00Z"),
    ]
    latest = deduplicate(parse(_bronze(spark, "orders", rows), spec), spec).collect()
    assert [(r.order_id, r.status) for r in latest] == [("ORD-2", "delivered")]
