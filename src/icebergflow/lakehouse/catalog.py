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
