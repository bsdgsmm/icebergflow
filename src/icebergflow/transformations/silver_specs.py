"""Silver table definitions: typed schema, parsing and validation rules per entity.

Each spec turns raw Bronze strings into typed columns and lists the conditions
that make a record invalid. The raw values stay available under ``_raw`` so a
rule can tell a missing value apart from one that failed to parse.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from pyspark.sql import functions as F

if TYPE_CHECKING:
    from pyspark.sql import Column

TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ssX"
ORDER_STATUSES = ("placed", "shipped", "delivered", "cancelled", "returned")
ORDER_CHANNELS = ("web", "mobile_app", "marketplace")
EMAIL_PATTERN = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"

Rule = tuple["Column", str]


@dataclass(frozen=True)
class Reference:
    """Foreign key: ``column`` must exist in ``table.target_column``."""

    column: str
    table: str
    target_column: str
    reason: str


@dataclass(frozen=True)
class SilverSpec:
    entity: str
    keys: tuple[str, ...]
    columns: str
    parse: Callable[[], list[Column]]
    rules: Callable[[], list[Rule]]
    version_column: str | None = None
    partitioned_by: tuple[str, ...] = ()
    references: tuple[Reference, ...] = field(default_factory=tuple)
    # Source columns introduced by a later source schema version. Batches that
    # predate them are read with the column set to NULL.
    optional_source_columns: tuple[str, ...] = ()

    @property
    def table(self) -> str:
        return f"silver.{self.entity}"

    @property
    def schema(self) -> list[tuple[str, str]]:
        """(name, type) pairs of ``columns``; splits only on top-level commas."""
        parts = re.split(r",(?![^()]*\))", self.columns)
        return [(name, dtype) for name, dtype in (p.strip().split(None, 1) for p in parts)]


def _raw(name: str) -> Column:
    return F.col(f"_raw.`{name}`")


def _ts(name: str) -> Column:
    return F.try_to_timestamp(F.col(name), F.lit(TIMESTAMP_FORMAT))


def _blank_to_null(name: str) -> Column:
    trimmed = F.trim(F.col(name))
    return F.when(trimmed == "", None).otherwise(trimmed)


def required(*names: str) -> list[Rule]:
    return [(_raw(n).isNull(), f"missing_{n}") for n in names]


def parseable(*names: str) -> list[Rule]:
    return [(_raw(n).isNotNull() & F.col(n).isNull(), f"invalid_{n}") for n in names]


def _customers_parse() -> list[Column]:
    return [
        _blank_to_null("customer_id").alias("customer_id"),
        F.col("first_name"),
        F.col("last_name"),
        F.lower(_blank_to_null("email")).alias("email"),
        F.upper(F.col("country")).alias("country"),
        F.col("signup_date").try_cast("date").alias("signup_date"),
        _ts("updated_at").alias("updated_at"),
    ]


def _customers_rules() -> list[Rule]:
    return [
        *required("customer_id", "email", "updated_at"),
        *parseable("signup_date", "updated_at"),
        (_raw("email").isNotNull() & ~F.col("email").rlike(EMAIL_PATTERN), "malformed_email"),
        (F.length("country") != 2, "invalid_country"),
    ]


def _products_parse() -> list[Column]:
    return [
        _blank_to_null("product_id").alias("product_id"),
        F.col("product_name"),
        F.col("category"),
        F.col("brand"),
        F.col("unit_price").try_cast("decimal(10,2)").alias("unit_price"),
        F.lower(F.col("is_active")).try_cast("boolean").alias("is_active"),
        _ts("updated_at").alias("updated_at"),
    ]


def _products_rules() -> list[Rule]:
    return [
        *required("product_id", "category", "unit_price", "updated_at"),
        *parseable("unit_price", "is_active", "updated_at"),
        (F.col("unit_price") < 0, "negative_unit_price"),
    ]


def _orders_parse() -> list[Column]:
    return [
        _blank_to_null("order_id").alias("order_id"),
        _blank_to_null("customer_id").alias("customer_id"),
        _ts("order_ts").alias("order_ts"),
        F.to_date(_ts("order_ts")).alias("order_date"),
        F.lower(F.col("status")).alias("status"),
        F.upper(F.col("currency")).alias("currency"),
        _ts("updated_at").alias("updated_at"),
        F.lower(_blank_to_null("channel")).alias("channel"),
        F.upper(_blank_to_null("coupon_code")).alias("coupon_code"),
    ]


def _orders_rules() -> list[Rule]:
    return [
        *required("order_id", "customer_id", "order_ts", "status", "updated_at"),
        *parseable("order_ts", "updated_at"),
        (_raw("status").isNotNull() & ~F.col("status").isin(*ORDER_STATUSES), "invalid_status"),
        (F.col("currency").isNotNull() & (F.length("currency") != 3), "invalid_currency"),
        (F.col("channel").isNotNull() & ~F.col("channel").isin(*ORDER_CHANNELS), "invalid_channel"),
    ]


def _order_items_parse() -> list[Column]:
    quantity = F.col("quantity").try_cast("int")
    unit_price = F.col("unit_price").try_cast("decimal(10,2)")
    discount = F.col("discount").try_cast("decimal(4,3)")
    gross = unit_price * quantity
    net = gross * (F.lit(1) - F.coalesce(discount, F.lit(0)))
    return [
        _blank_to_null("order_id").alias("order_id"),
        F.col("line_number").try_cast("int").alias("line_number"),
        _blank_to_null("product_id").alias("product_id"),
        quantity.alias("quantity"),
        unit_price.alias("unit_price"),
        F.coalesce(discount, F.lit(0)).cast("decimal(4,3)").alias("discount"),
        F.round(net, 2).cast("decimal(12,2)").alias("line_amount"),
    ]


def _order_items_rules() -> list[Rule]:
    return [
        *required("order_id", "line_number", "product_id", "quantity", "unit_price"),
        *parseable("line_number", "quantity", "unit_price", "discount"),
        (F.col("line_number") < 1, "invalid_line_number"),
        (F.col("quantity") <= 0, "non_positive_quantity"),
        (F.col("unit_price") < 0, "negative_unit_price"),
        ((F.col("discount") < 0) | (F.col("discount") >= 1), "invalid_discount"),
    ]


SPECS: dict[str, SilverSpec] = {
    "customers": SilverSpec(
        entity="customers",
        keys=("customer_id",),
        columns=(
            "customer_id STRING, first_name STRING, last_name STRING, email STRING, "
            "country STRING, signup_date DATE, updated_at TIMESTAMP, "
            "_batch_id STRING, _processed_at TIMESTAMP"
        ),
        parse=_customers_parse,
        rules=_customers_rules,
        version_column="updated_at",
    ),
    "products": SilverSpec(
        entity="products",
        keys=("product_id",),
        columns=(
            "product_id STRING, product_name STRING, category STRING, brand STRING, "
            "unit_price DECIMAL(10,2), is_active BOOLEAN, updated_at TIMESTAMP, "
            "_batch_id STRING, _processed_at TIMESTAMP"
        ),
        parse=_products_parse,
        rules=_products_rules,
        version_column="updated_at",
    ),
    "orders": SilverSpec(
        entity="orders",
        keys=("order_id",),
        columns=(
            "order_id STRING, customer_id STRING, order_ts TIMESTAMP, order_date DATE, "
            "status STRING, currency STRING, updated_at TIMESTAMP, "
            "channel STRING, coupon_code STRING, "
            "_batch_id STRING, _processed_at TIMESTAMP"
        ),
        parse=_orders_parse,
        rules=_orders_rules,
        version_column="updated_at",
        partitioned_by=("days(order_ts)",),
        references=(
            Reference("customer_id", "silver.customers", "customer_id", "unknown_customer"),
        ),
        optional_source_columns=("channel", "coupon_code"),
    ),
    "order_items": SilverSpec(
        entity="order_items",
        keys=("order_id", "line_number"),
        columns=(
            "order_id STRING, line_number INT, product_id STRING, quantity INT, "
            "unit_price DECIMAL(10,2), discount DECIMAL(4,3), line_amount DECIMAL(12,2), "
            "_batch_id STRING, _processed_at TIMESTAMP"
        ),
        parse=_order_items_parse,
        rules=_order_items_rules,
        references=(
            Reference("order_id", "silver.orders", "order_id", "unknown_order"),
            Reference("product_id", "silver.products", "product_id", "unknown_product"),
        ),
    ),
}

QUARANTINE_TABLE = "silver.quarantine"
QUARANTINE_COLUMNS = (
    "entity STRING, batch_id STRING, record_hash STRING, reasons ARRAY<STRING>, "
    "record STRING, quarantined_at TIMESTAMP"
)
