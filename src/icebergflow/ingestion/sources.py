"""Source batch layout: which files exist, what they contain and what the producer promised."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path

from icebergflow.datagen import (
    CUSTOMER_COLUMNS,
    ORDER_COLUMNS_V1,
    ORDER_ITEM_COLUMNS,
    PRODUCT_COLUMNS,
)

BATCH_ID_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}(-[a-z0-9]+)?$")
MANIFEST_FILE = "_manifest.json"


@dataclass(frozen=True)
class SourceEntity:
    name: str
    filename: str
    file_format: str
    columns: tuple[str, ...]


ENTITIES: dict[str, SourceEntity] = {
    "customers": SourceEntity("customers", "customers.csv", "csv", tuple(CUSTOMER_COLUMNS)),
    "products": SourceEntity("products", "products.csv", "csv", tuple(PRODUCT_COLUMNS)),
    "orders": SourceEntity("orders", "orders.jsonl", "json", tuple(ORDER_COLUMNS_V1)),
    "order_items": SourceEntity(
        "order_items", "order_items.jsonl", "json", tuple(ORDER_ITEM_COLUMNS)
    ),
}


@dataclass(frozen=True)
class SourceBatch:
    batch_id: str
    path: Path

    def file(self, entity: SourceEntity) -> Path:
        return self.path / entity.filename

    def manifest(self) -> dict | None:
        path = self.path / MANIFEST_FILE
        if not path.is_file():
            return None
        return json.loads(path.read_text(encoding="utf-8"))

    def expected_rows(self, entity: SourceEntity) -> int | None:
        manifest = self.manifest()
        if manifest is None:
            return None
        return manifest.get("row_counts", {}).get(entity.name)


def discover_batches(raw_dir: Path, batch_ids: list[str] | None = None) -> list[SourceBatch]:
    """Batches under ``raw_dir`` in ascending batch id order (which is load order)."""
    if not raw_dir.is_dir():
        raise FileNotFoundError(f"raw data directory not found: {raw_dir}")
    found = {
        p.name: SourceBatch(p.name, p)
        for p in raw_dir.iterdir()
        if p.is_dir() and BATCH_ID_PATTERN.match(p.name)
    }
    if batch_ids:
        unknown = sorted(set(batch_ids) - set(found))
        if unknown:
            raise FileNotFoundError(f"batches not found in {raw_dir}: {', '.join(unknown)}")
        found = {k: v for k, v in found.items() if k in batch_ids}
    return [found[k] for k in sorted(found)]
