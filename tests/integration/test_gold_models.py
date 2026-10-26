"""Gold model correctness on a small hand-built Silver dataset with known answers."""

from datetime import date
from decimal import Decimal

import pytest

from icebergflow.transformations.gold import MODELS, run_gold
from icebergflow.transformations.silver import ensure_silver_tables

CUSTOMERS = [
    ("C1", "TR", "2023-12-20"),
    ("C2", "US", "2023-12-28"),
    ("C3", "DE", "2024-01-02"),
]
PRODUCTS = [
    ("P1", "Books", "10.00"),
    ("P2", "Books", "20.00"),
    ("P3", "Grocery", "5.00"),
]
ORDERS = [
    ("O1", "C1", "2024-01-01 09:00:00", "delivered"),
    ("O2", "C1", "2024-01-05 12:00:00", "shipped"),
    ("O3", "C2", "2024-01-01 15:00:00", "cancelled"),
    ("O4", "C2", "2024-01-05 18:00:00", "placed"),
    ("O5", "C2", "2024-02-03 10:00:00", "returned"),
]
ITEMS = [
    ("O1", 1, "P1", 2, "10.00", "0.000", "20.00"),
    ("O1", 2, "P3", 1, "5.00", "0.000", "5.00"),
    ("O2", 1, "P2", 1, "20.00", "0.100", "18.00"),
    ("O3", 1, "P1", 1, "10.00", "0.000", "10.00"),
    ("O4", 1, "P3", 4, "5.00", "0.000", "20.00"),
    ("O5", 1, "P2", 1, "20.00", "0.000", "20.00"),
]


@pytest.fixture
def gold(spark, settings, lakehouse):
    ensure_silver_tables(spark)
    lineage = "'b1', current_timestamp()"
    spark.sql(
        "INSERT INTO silver.customers VALUES "
        + ", ".join(
            f"('{c}', 'First', 'Last', '{c.lower()}@example.com', '{country}', "
            f"date'{signup}', timestamp'{signup} 00:00:00', {lineage})"
            for c, country, signup in CUSTOMERS
        )
    )
    spark.sql(
        "INSERT INTO silver.products VALUES "
        + ", ".join(
            f"('{p}', 'Product {p}', '{cat}', 'Brand', {price}, true, "
            f"timestamp'2024-01-01 00:00:00', {lineage})"
            for p, cat, price in PRODUCTS
        )
    )
    spark.sql(
        "INSERT INTO silver.orders VALUES "
        + ", ".join(
            f"('{o}', '{c}', timestamp'{ts}', date'{ts[:10]}', '{status}', 'USD', "
            f"timestamp'{ts}', {lineage})"
            for o, c, ts, status in ORDERS
        )
    )
    spark.sql(
        "INSERT INTO silver.order_items VALUES "
        + ", ".join(
            f"('{o}', {n}, '{p}', {q}, {price}, {disc}, {amount}, {lineage})"
            for o, n, p, q, price, disc, amount in ITEMS
        )
    )
    run_gold(spark, settings.sql_dir)
    return spark


def _rows(spark, table, order_by):
    return [r.asDict() for r in spark.table(table).orderBy(*order_by).collect()]


def test_daily_sales(gold):
    rows = _rows(gold, "gold.daily_sales", ["order_date"])
    assert [
        (
            r["order_date"],
            r["orders"],
            r["net_orders"],
            r["cancelled_orders"],
            r["returned_orders"],
            r["unique_customers"],
            r["units_sold"],
            r["gross_revenue"],
            r["net_revenue"],
            r["avg_order_value"],
        )
        for r in rows
    ] == [
        (date(2024, 1, 1), 2, 1, 1, 0, 2, 3, Decimal("35.00"), Decimal("25.00"), Decimal("25.00")),
        (date(2024, 1, 5), 2, 2, 0, 0, 2, 5, Decimal("38.00"), Decimal("38.00"), Decimal("19.00")),
        (date(2024, 2, 3), 1, 0, 0, 1, 1, 0, Decimal("20.00"), Decimal("0.00"), None),
    ]


def test_product_performance(gold):
    rows = _rows(gold, "gold.product_performance", ["product_id"])
    assert [
        (r["product_id"], r["units_sold"], r["orders"], r["net_revenue"],
         r["category_revenue_rank"])
        for r in rows
    ] == [
        ("P1", 2, 1, Decimal("20.00"), 1),
        ("P2", 1, 1, Decimal("18.00"), 2),
        ("P3", 5, 2, Decimal("25.00"), 1),
    ]  # fmt: skip
    assert rows[2]["first_sold_date"] == date(2024, 1, 1)
    assert rows[2]["last_sold_date"] == date(2024, 1, 5)


def test_category_monthly(gold):
    rows = _rows(gold, "gold.category_monthly", ["order_month", "category"])
    assert [
        (r["order_month"], r["category"], r["units_sold"], r["orders"], r["net_revenue"],
         r["revenue_share"])
        for r in rows
    ] == [
        (date(2024, 1, 1), "Books", 3, 2, Decimal("38.00"), Decimal("0.6032")),
        (date(2024, 1, 1), "Grocery", 5, 2, Decimal("25.00"), Decimal("0.3968")),
    ]  # fmt: skip


def test_customer_summary(gold):
    rows = {r["customer_id"]: r for r in _rows(gold, "gold.customer_summary", ["customer_id"])}
    c1, c2, c3 = rows["C1"], rows["C2"], rows["C3"]

    assert (c1["order_count"], c1["lifetime_revenue"], c1["avg_order_value"]) == (
        2,
        Decimal("43.00"),
        Decimal("21.50"),
    )
    assert c1["is_repeat_customer"] is True
    assert c1["days_to_second_order"] == 4
    assert (c2["order_count"], c2["lifetime_revenue"], c2["is_repeat_customer"]) == (
        1,
        Decimal("20.00"),
        False,
    )
    assert c2["first_order_date"] == date(2024, 1, 5)
    assert (c3["order_count"], c3["lifetime_revenue"], c3["first_order_date"]) == (
        0,
        Decimal("0.00"),
        None,
    )


def test_repeat_purchase_cohorts(gold):
    rows = _rows(gold, "gold.repeat_purchase_cohorts", ["cohort_month"])
    assert len(rows) == 1
    cohort = rows[0]
    assert cohort["cohort_month"] == date(2024, 1, 1)
    assert (cohort["cohort_customers"], cohort["repeat_customers"]) == (2, 1)
    assert cohort["repeat_rate"] == Decimal("0.5000")
    assert cohort["repeat_within_7_days"] == 1
    assert cohort["avg_orders_per_customer"] == Decimal("1.50")
    assert cohort["avg_days_to_second_order"] == Decimal("4.00")


def test_rebuild_replaces_content_in_one_snapshot(gold, settings):
    spark = gold
    before = spark.sql("SELECT count(*) AS n FROM gold.daily_sales.snapshots").first().n
    spark.sql("UPDATE silver.orders SET status = 'cancelled' WHERE order_id = 'O4'")

    run_gold(spark, settings.sql_dir, ["daily_sales"])

    after = spark.sql("SELECT operation FROM gold.daily_sales.snapshots").collect()
    assert len(after) == before + 1
    jan5 = spark.table("gold.daily_sales").where("order_date = date'2024-01-05'").first()
    assert (jan5.net_orders, jan5.net_revenue) == (1, Decimal("18.00"))


def test_grain_columns_are_unique(gold):
    for model in MODELS:
        keys = ", ".join(model.grain)
        dupes = gold.sql(
            f"SELECT {keys}, count(*) c FROM {model.table} GROUP BY {keys} HAVING c > 1"
        ).count()
        assert dupes == 0, model.table
