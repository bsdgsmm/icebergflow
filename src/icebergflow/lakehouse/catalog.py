"""Iceberg catalog helpers: namespaces, table creation and snapshot metadata."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

DEFAULT_TABLE_PROPERTIES: dict[str, str] = {
    "format-version": "2",
    "write.format.default": "parquet",
    "write.parquet.compression-codec": "zstd",
    # Bound metadata growth: every commit writes a new metadata.json.
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "100",
}


@dataclass(frozen=True)
class Snapshot:
    snapshot_id: int
    parent_id: int | None
    committed_at: datetime
    operation: str
    summary: dict[str, str]


def ensure_namespace(spark: SparkSession, namespace: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")


def table_exists(spark: SparkSession, identifier: str) -> bool:
    return spark.catalog.tableExists(identifier)


def create_table(
    spark: SparkSession,
    identifier: str,
    columns: str,
    partitioned_by: Iterable[str] = (),
    properties: Mapping[str, str] | None = None,
) -> bool:
    """Create an Iceberg table if it does not exist. Returns True when it was created."""
    if table_exists(spark, identifier):
        return False
    namespace = identifier.rsplit(".", 1)[0]
    ensure_namespace(spark, namespace)
    props = {**DEFAULT_TABLE_PROPERTIES, **(properties or {})}
    partitions = list(partitioned_by)
    partition_clause = f"PARTITIONED BY ({', '.join(partitions)})" if partitions else ""
    tblproperties = ", ".join(f"'{k}'='{v}'" for k, v in props.items())
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {identifier} ({columns}) USING iceberg "
        f"{partition_clause} TBLPROPERTIES ({tblproperties})"
    )
    return True


def snapshots(spark: SparkSession, identifier: str) -> list[Snapshot]:
    rows = spark.sql(
        f"SELECT snapshot_id, parent_id, committed_at, operation, summary "
        f"FROM {identifier}.snapshots ORDER BY committed_at, snapshot_id"
    ).collect()
    return [
        Snapshot(
            snapshot_id=row.snapshot_id,
            parent_id=row.parent_id,
            committed_at=row.committed_at,
            operation=row.operation,
            summary=dict(row.summary or {}),
        )
        for row in rows
    ]


def current_snapshot_id(spark: SparkSession, identifier: str) -> int | None:
    """Snapshot id of the table's current state (follows rollbacks, unlike the latest commit)."""
    row = spark.sql(
        f"SELECT snapshot_id FROM {identifier}.history WHERE is_current_ancestor "
        f"ORDER BY made_current_at DESC LIMIT 1"
    ).first()
    return None if row is None else row.snapshot_id


def current_lineage(spark: SparkSession, identifier: str) -> list[tuple[int, str]]:
    """(snapshot_id, operation) of every ancestor of the current state, oldest first."""
    rows = spark.sql(
        f"SELECT h.snapshot_id, s.operation FROM {identifier}.history h "
        f"JOIN {identifier}.snapshots s ON s.snapshot_id = h.snapshot_id "
        f"WHERE h.is_current_ancestor ORDER BY h.made_current_at, h.snapshot_id"
    ).collect()
    return [(row.snapshot_id, row.operation) for row in rows]


def appended_since(spark: SparkSession, identifier: str, snapshot_id: int) -> bool:
    """True if the current state descends from ``snapshot_id`` through appends only.

    That is the precondition for an incremental append scan: if the table was
    rolled back past the snapshot, or rows were overwritten or deleted after it,
    the rows between the two snapshots are not a simple set of new records.
    """
    lineage = current_lineage(spark, identifier)
    ids = [snapshot for snapshot, _ in lineage]
    if snapshot_id not in ids:
        return False
    later = lineage[ids.index(snapshot_id) + 1 :]
    return all(operation == "append" for _, operation in later)


def add_missing_columns(
    spark: SparkSession, identifier: str, columns: list[tuple[str, str]]
) -> list[str]:
    """Add declared columns missing from the table, each placed after its predecessor.

    Iceberg column additions are metadata-only: existing data files are not
    rewritten and read the new columns as NULL.
    """
    existing = set(spark.table(identifier).columns)
    added = []
    previous = None
    for name, dtype in columns:
        if name not in existing:
            position = f" AFTER `{previous}`" if previous else " FIRST"
            spark.sql(f"ALTER TABLE {identifier} ADD COLUMN `{name}` {dtype}{position}")
            added.append(name)
        previous = name
    return added


def partition_fields(spark: SparkSession, identifier: str) -> list[str]:
    """Current partition spec as Spark transform expressions, e.g. ``['days(order_ts)']``."""
    rows = spark.sql(f"DESCRIBE TABLE {identifier}").collect()
    fields, in_section = [], False
    for row in rows:
        if row.col_name.strip() == "# Partitioning":
            in_section = True
            continue
        if in_section:
            if not row.col_name.startswith("Part "):
                break
            fields.append(row.data_type.strip())
    return fields


def add_partition_field(spark: SparkSession, identifier: str, transform: str) -> bool:
    """Evolve the partition spec. Existing files keep their old spec; returns False if present."""
    normalized = transform.replace(" ", "").lower()
    if normalized in {f.replace(" ", "").lower() for f in partition_fields(spark, identifier)}:
        return False
    spark.sql(f"ALTER TABLE {identifier} ADD PARTITION FIELD {transform}")
    return True
