"""Deterministic synthetic e-commerce data generator.

Each generated day is one source batch, laid out the way an operational system
would export it:

* ``customers.csv`` and ``products.csv`` carry new and changed rows (CDC style).
* ``orders.jsonl`` carries newly placed orders plus status changes of earlier
  orders, so the same ``order_id`` appears several times over its lifecycle.
* ``order_items.jsonl`` carries order lines for newly placed orders.
* ``_manifest.json`` records row counts and every anomaly that was injected.

A small, configurable share of records is corrupted on purpose (missing keys,
unparseable timestamps, orphaned references, negative quantities) and some
records are re-sent unchanged, so the Silver layer has real defects to handle.
All randomness flows from a single seeded ``random.Random`` instance, which
makes the output byte-for-byte reproducible for a given configuration.
"""

from __future__ import annotations

import csv
import json
import logging
import random
from collections import Counter
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)

CUSTOMER_COLUMNS = [
    "customer_id",
    "first_name",
    "last_name",
    "email",
    "country",
    "signup_date",
    "updated_at",
]
PRODUCT_COLUMNS = [
    "product_id",
    "product_name",
    "category",
    "brand",
    "unit_price",
    "is_active",
    "updated_at",
]
ORDER_COLUMNS_V1 = ["order_id", "customer_id", "order_ts", "status", "currency", "updated_at"]
ORDER_COLUMNS_V2 = [*ORDER_COLUMNS_V1, "channel", "coupon_code"]
ORDER_ITEM_COLUMNS = ["order_id", "line_number", "product_id", "quantity", "unit_price", "discount"]

ORDER_STATUSES = ("placed", "shipped", "delivered", "cancelled", "returned")
CHANNELS = ("web", "mobile_app", "marketplace")
COUPONS = ("WELCOME10", "SPRING15", "LOYAL20", "FREESHIP")

UNKNOWN_CUSTOMER_ID = "CUS-999999"
UNKNOWN_PRODUCT_ID = "PRD-9999"

CATEGORIES: dict[str, tuple[tuple[str, ...], tuple[float, float]]] = {
    "Electronics": (
        ("Wireless Earbuds", "Bluetooth Speaker", "USB-C Hub", "Smartwatch", "Portable SSD"),
        (19.0, 249.0),
    ),
    "Home & Kitchen": (
        ("Chef Knife", "French Press", "Air Fryer", "Cast Iron Skillet", "Storage Set"),
        (12.0, 160.0),
    ),
    "Books": (
        ("Data Engineering Handbook", "SQL Cookbook", "Mystery Novel", "Travel Guide", "Atlas"),
        (8.0, 55.0),
    ),
    "Sports & Outdoors": (
        ("Yoga Mat", "Trail Backpack", "Water Bottle", "Resistance Bands", "Camping Lantern"),
        (10.0, 140.0),
    ),
    "Beauty": (
        ("Face Serum", "Shampoo Bar", "Sunscreen SPF50", "Lip Balm Set", "Hair Dryer"),
        (6.0, 90.0),
    ),
    "Toys & Games": (
        ("Puzzle 1000pc", "Board Game", "Building Blocks", "RC Car", "Card Game"),
        (9.0, 120.0),
    ),
    "Clothing": (
        ("Running Shoes", "Rain Jacket", "Wool Socks", "Denim Jeans", "Hoodie"),
        (12.0, 150.0),
    ),
    "Grocery": (
        ("Coffee Beans 1kg", "Green Tea", "Olive Oil", "Dark Chocolate", "Granola"),
        (4.0, 35.0),
    ),
}
BRANDS = ("Northwind", "Contoso", "Fabrikam", "Tailspin", "Adventure Works", "Litware")
FIRST_NAMES = (
    "Ava", "Liam", "Noah", "Emma", "Mia", "Lucas", "Zeynep", "Elif", "Omar", "Sofia",
    "Mateo", "Hana", "Arjun", "Chloe", "Leon", "Yuki", "Amara", "Ivan", "Nora", "Diego",
)  # fmt: skip
LAST_NAMES = (
    "Smith", "Garcia", "Kaya", "Yilmaz", "Muller", "Rossi", "Tanaka", "Silva", "Novak",
    "Khan", "Dubois", "Jensen", "Ortiz", "Kim", "Nowak", "Costa", "Brown", "Sato",
)  # fmt: skip
COUNTRIES = ("US", "DE", "TR", "GB", "FR", "NL", "ES", "JP")
COUNTRY_WEIGHTS = (30, 15, 15, 12, 10, 7, 6, 5)


ORDER_ANOMALIES: dict[str, Callable[[dict], None]] = {
    "missing_customer_id": lambda r: r.update(customer_id=None),
    "unknown_customer": lambda r: r.update(customer_id=UNKNOWN_CUSTOMER_ID),
    "malformed_order_ts": lambda r: r.update(
        order_ts=r["order_ts"].replace("T", " ").replace("-", "/")
    ),
    "invalid_status": lambda r: r.update(status="unknown"),
}
ORDER_ITEM_ANOMALIES: dict[str, Callable[[dict], None]] = {
    "negative_quantity": lambda r: r.update(quantity=-r["quantity"]),
    "unknown_product": lambda r: r.update(product_id=UNKNOWN_PRODUCT_ID),
    "null_price": lambda r: r.update(unit_price=None),
}


@dataclass(frozen=True)
class GeneratorConfig:
    start_date: date = date(2024, 1, 1)
    days: int = 14
    orders_per_day: int = 150
    initial_customers: int = 300
    daily_new_customers: int = 12
    products: int = 80
    seed: int = 42
    schema_v2_from: date | None = None
    anomaly_rate: float = 0.02
    duplicate_rate: float = 0.03

    def __post_init__(self) -> None:
        if self.days < 1:
            raise ValueError("days must be at least 1")
        if self.orders_per_day < 1 or self.initial_customers < 1 or self.products < 1:
            raise ValueError("orders_per_day, initial_customers and products must be positive")
        if self.daily_new_customers < 0:
            raise ValueError("daily_new_customers cannot be negative")
        for name in ("anomaly_rate", "duplicate_rate"):
            if not 0.0 <= getattr(self, name) < 0.5:
                raise ValueError(f"{name} must be in [0, 0.5)")

    def schema_version(self, day: date) -> int:
        return 2 if self.schema_v2_from is not None and day >= self.schema_v2_from else 1


@dataclass
class BatchSummary:
    batch_id: str
    path: Path
    schema_version: int
    row_counts: dict[str, int]
    injected_anomalies: dict[str, int]
    duplicates: dict[str, int] = field(default_factory=dict)


def _ts(day: date, seconds: int) -> str:
    moment = datetime.combine(day, time(), tzinfo=UTC) + timedelta(seconds=seconds)
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


class _Generator:
    def __init__(self, config: GeneratorConfig) -> None:
        self.config = config
        self.rng = random.Random(config.seed)
        self.customers: dict[str, dict] = {}
        self.customer_weights: dict[str, int] = {}
        self.products: dict[str, dict] = {}
        self.product_weights: dict[str, float] = {}
        self.order_seq = 0
        self.customer_seq = 0
        # Status transitions planned when an order is placed, keyed by the day they fire.
        self.scheduled: dict[date, list[tuple[str, str]]] = {}
        self.orders: dict[str, dict] = {}
        self.emitted_orders: list[dict] = []

    def _new_customer(self, day: date) -> dict:
        self.customer_seq += 1
        customer_id = f"CUS-{self.customer_seq:06d}"
        first = self.rng.choice(FIRST_NAMES)
        last = self.rng.choice(LAST_NAMES)
        record = {
            "customer_id": customer_id,
            "first_name": first,
            "last_name": last,
            "email": f"{first}.{last}.{self.customer_seq}@example.com".lower(),
            "country": self.rng.choices(COUNTRIES, COUNTRY_WEIGHTS)[0],
            "signup_date": day.isoformat(),
            "updated_at": _ts(day, self.rng.randrange(86_400)),
        }
        self.customers[customer_id] = record
        # A minority of customers buy far more often, which gives the repeat
        # purchase analysis in Gold a realistic long tail to work with.
        self.customer_weights[customer_id] = self.rng.choice((1, 1, 1, 2, 6))
        return dict(record)

    def _seed_products(self, day: date) -> list[dict]:
        rows = []
        categories = list(CATEGORIES)
        for seq in range(1, self.config.products + 1):
            category = categories[(seq - 1) % len(categories)]
            items, (low, high) = CATEGORIES[category]
            product_id = f"PRD-{seq:04d}"
            brand = self.rng.choice(BRANDS)
            record = {
                "product_id": product_id,
                "product_name": f"{brand} {self.rng.choice(items)}",
                "category": category,
                "brand": brand,
                "unit_price": f"{self.rng.uniform(low, high):.2f}",
                "is_active": "true",
                "updated_at": _ts(day, self.rng.randrange(3_600)),
            }
            self.products[product_id] = record
            self.product_weights[product_id] = 1.0 / (1 + self.rng.random() * 9)
            rows.append(dict(record))
        return rows

    def _customer_updates(self, day: date, anomalies: Counter) -> list[dict]:
        rows = []
        for customer_id in list(self.customers):
            if self.customers[customer_id]["signup_date"] == day.isoformat():
                continue
            if self.rng.random() >= 0.02:
                continue
            record = dict(self.customers[customer_id])
            record["updated_at"] = _ts(day, self.rng.randrange(86_400))
            if self.rng.random() < self.config.anomaly_rate * 5:
                record["email"] = record["email"].replace("@", "_at_")
                anomalies["customers.malformed_email"] += 1
                rows.append(record)
                continue
            record["country"] = self.rng.choices(COUNTRIES, COUNTRY_WEIGHTS)[0]
            self.customers[customer_id] = record
            rows.append(dict(record))
        return rows

    def _product_updates(self, day: date, anomalies: Counter) -> list[dict]:
        rows = []
        for product_id, current in self.products.items():
            if self.rng.random() >= 0.02:
                continue
            record = dict(current)
            record["updated_at"] = _ts(day, self.rng.randrange(86_400))
            if self.rng.random() < self.config.anomaly_rate * 5:
                record["unit_price"] = f"-{record['unit_price']}"
                anomalies["products.negative_price"] += 1
                rows.append(record)
                continue
            factor = self.rng.choice((0.85, 0.9, 1.05, 1.1))
            record["unit_price"] = f"{float(current['unit_price']) * factor:.2f}"
            self.products[product_id] = record
            rows.append(dict(record))
        return rows

    def _order_record(self, base: dict, status: str, updated_at: str, version: int) -> dict:
        record = {
            "order_id": base["order_id"],
            "customer_id": base["customer_id"],
            "order_ts": base["order_ts"],
            "status": status,
            "currency": "USD",
            "updated_at": updated_at,
        }
        if version == 2:
            record["channel"] = base.get("channel")
            record["coupon_code"] = base.get("coupon_code")
        return record

    def _schedule_lifecycle(self, order_id: str, day: date) -> None:
        roll = self.rng.random()
        if roll < 0.05:
            self.scheduled.setdefault(day + timedelta(days=1), []).append((order_id, "cancelled"))
            return
        shipped = day + timedelta(days=self.rng.randint(1, 2))
        delivered = shipped + timedelta(days=self.rng.randint(1, 4))
        self.scheduled.setdefault(shipped, []).append((order_id, "shipped"))
        self.scheduled.setdefault(delivered, []).append((order_id, "delivered"))
        if roll > 0.96:
            returned = delivered + timedelta(days=self.rng.randint(3, 7))
            self.scheduled.setdefault(returned, []).append((order_id, "returned"))

    def _new_orders(
        self, day: date, version: int, anomalies: Counter
    ) -> tuple[list[dict], list[dict]]:
        orders: list[dict] = []
        items: list[dict] = []
        customer_ids = list(self.customer_weights)
        weights = [self.customer_weights[c] for c in customer_ids]
        product_ids = list(self.product_weights)
        product_weights = [self.product_weights[p] for p in product_ids]
        spread = max(1, self.config.orders_per_day // 5)
        count = self.config.orders_per_day + self.rng.randint(-spread, spread)

        for _ in range(max(1, count)):
            self.order_seq += 1
            base = {
                "order_id": f"ORD-{self.order_seq:07d}",
                "customer_id": self.rng.choices(customer_ids, weights)[0],
                "order_ts": _ts(day, self.rng.randrange(86_400)),
            }
            if version == 2:
                base["channel"] = self.rng.choices(CHANNELS, (55, 35, 10))[0]
                base["coupon_code"] = self.rng.choice(COUPONS) if self.rng.random() < 0.15 else None
            record = self._order_record(base, "placed", base["order_ts"], version)

            clean = True
            if self.rng.random() < self.config.anomaly_rate:
                clean = False
                kind = self.rng.choice(list(ORDER_ANOMALIES))
                ORDER_ANOMALIES[kind](record)
                anomalies[f"orders.{kind}"] += 1
            orders.append(record)

            if clean:
                self.orders[base["order_id"]] = base
                self.emitted_orders.append(record)
                self._schedule_lifecycle(base["order_id"], day)

            for line_number in range(1, self.rng.choice((1, 1, 2, 2, 3, 4)) + 1):
                product_id = self.rng.choices(product_ids, product_weights)[0]
                item = {
                    "order_id": base["order_id"],
                    "line_number": line_number,
                    "product_id": product_id,
                    "quantity": self.rng.choice((1, 1, 1, 2, 2, 3)),
                    "unit_price": float(self.products[product_id]["unit_price"]),
                    "discount": self.rng.choice((0.0, 0.0, 0.0, 0.0, 0.1, 0.2)),
                }
                if clean and self.rng.random() < self.config.anomaly_rate:
                    kind = self.rng.choice(list(ORDER_ITEM_ANOMALIES))
                    ORDER_ITEM_ANOMALIES[kind](item)
                    anomalies[f"order_items.{kind}"] += 1
                items.append(item)
        return orders, items

    def _status_changes(self, day: date, version: int) -> list[dict]:
        rows = []
        for order_id, status in self.scheduled.pop(day, []):
            base = self.orders[order_id]
            updated_at = _ts(day, self.rng.randrange(86_400))
            record = self._order_record(base, status, updated_at, version)
            rows.append(record)
            self.emitted_orders.append(record)
        return rows

    def _with_duplicates(self, rows: list[dict]) -> tuple[list[dict], int]:
        duplicates = [dict(r) for r in rows if self.rng.random() < self.config.duplicate_rate]
        return rows + duplicates, len(duplicates)

    def batch(self, index: int, day: date) -> tuple[dict[str, list[dict]], Counter, dict]:
        version = self.config.schema_version(day)
        anomalies: Counter = Counter()

        if index == 0:
            products = self._seed_products(day)
            customers = [self._new_customer(day) for _ in range(self.config.initial_customers)]
        else:
            products = self._product_updates(day, anomalies)
            customers = self._customer_updates(day, anomalies)
            customers += [self._new_customer(day) for _ in range(self.config.daily_new_customers)]

        resend_pool = list(self.emitted_orders)
        changes = self._status_changes(day, version)
        placed, items = self._new_orders(day, version, anomalies)
        orders, order_dupes = self._with_duplicates(changes + placed)
        items, item_dupes = self._with_duplicates(items)

        # Late re-deliveries of older order events, as an at-least-once
        # producer would send after a retry. They must never roll state back.
        resent = 0
        if resend_pool:
            for _ in range(round(len(placed) * self.config.duplicate_rate / 2)):
                old = dict(self.rng.choice(resend_pool))
                if version == 2:
                    old.setdefault("channel", None)
                    old.setdefault("coupon_code", None)
                else:
                    old.pop("channel", None)
                    old.pop("coupon_code", None)
                orders.append(old)
                resent += 1

        for rows in (orders, items, customers, products):
            self.rng.shuffle(rows)
        duplicates = {"orders": order_dupes, "order_items": item_dupes, "orders_resent": resent}
        return (
            {"customers": customers, "products": products, "orders": orders,
             "order_items": items},
            anomalies,
            duplicates,
        )  # fmt: skip


def _write_csv(path: Path, columns: list[str], rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _write_jsonl(path: Path, columns: list[str], rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        for row in rows:
            ordered = {column: row.get(column) for column in columns}
            handle.write(json.dumps(ordered, separators=(",", ":")) + "\n")


def generate(config: GeneratorConfig, output_dir: Path) -> list[BatchSummary]:
    """Write ``config.days`` daily batches under ``output_dir/<batch_id>/``."""
    generator = _Generator(config)
    summaries = []
    for index in range(config.days):
        day = config.start_date + timedelta(days=index)
        batch_id = day.isoformat()
        version = config.schema_version(day)
        tables, anomalies, duplicates = generator.batch(index, day)

        batch_dir = output_dir / batch_id
        batch_dir.mkdir(parents=True, exist_ok=True)
        order_columns = ORDER_COLUMNS_V2 if version == 2 else ORDER_COLUMNS_V1
        _write_csv(batch_dir / "customers.csv", CUSTOMER_COLUMNS, tables["customers"])
        _write_csv(batch_dir / "products.csv", PRODUCT_COLUMNS, tables["products"])
        _write_jsonl(batch_dir / "orders.jsonl", order_columns, tables["orders"])
        _write_jsonl(batch_dir / "order_items.jsonl", ORDER_ITEM_COLUMNS, tables["order_items"])

        row_counts = {name: len(rows) for name, rows in tables.items()}
        manifest = {
            "batch_id": batch_id,
            "schema_version": version,
            "row_counts": row_counts,
            "injected_anomalies": dict(sorted(anomalies.items())),
            "duplicates": duplicates,
            "generator": {
                k: (v.isoformat() if isinstance(v, date) else v) for k, v in asdict(config).items()
            },
        }
        (batch_dir / "_manifest.json").write_text(
            json.dumps(manifest, indent=2) + "\n", encoding="utf-8", newline="\n"
        )
        summaries.append(
            BatchSummary(batch_id, batch_dir, version, row_counts, dict(anomalies), duplicates)
        )
        logger.info("generated batch", extra={"batch_id": batch_id, **row_counts})
    return summaries
