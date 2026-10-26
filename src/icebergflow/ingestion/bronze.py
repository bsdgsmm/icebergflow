"""Bronze layer: land raw source files in Iceberg without losing or reinterpreting data.

Every source column is stored as STRING exactly as delivered, so values that
later fail validation are still available for inspection. Each row carries
lineage columns (``_batch_id``, ``_source_file``, ``_ingested_at``,
``_record_hash``).

Schema evolution: a batch that carries new source columns adds them to the
table (as STRING, after the existing source columns) before it is appended.
Iceberg schema changes are metadata-only, so earlier files are not rewritten
and read the new columns as NULL; snapshots taken before the change keep their
original schema for time travel. Columns that disappear from a batch are
stored as NULL. Column names must be lower snake case.

Idempotency: each append commits the batch id as an Iceberg snapshot property
(``icebergflow.batch-id``). Data and marker land in the same atomic commit, so
a batch is either fully present with its marker or absent. Reruns skip batches
whose marker exists in the table's current lineage, and a crash at any point
can simply be retried. Rolling a table back to an earlier snapshot also rolls
back the markers, so rolled-back batches are reloaded on the next run.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

from icebergflow.ingestion.sources import ENTITIES, SourceBatch, SourceEntity, discover_batches
from icebergflow.lakehouse.catalog import create_table

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from icebergflow.config import Settings

logger = logging.getLogger(__name__)

NAMESPACE = "bronze"
VALID_COLUMN_NAME = re.compile(r"^[a-z][a-z0-9_]*$")
BATCH_PROPERTY = "icebergflow.batch-id"
METADATA_COLUMNS = (
    ("_batch_id", "STRING"),
    ("_source_file", "STRING"),
    ("_ingested_at", "TIMESTAMP"),
    ("_record_hash", "STRING"),
    ("_corrupt_record", "STRING"),
)


class IngestionError(RuntimeError):
    pass


class SchemaDriftError(IngestionError):
    pass


@dataclass(frozen=True)
class IngestionResult:
    batch_id: str
    entity: str
    status: str
    rows: int


def bronze_table(entity: str) -> str:
    return f"{NAMESPACE}.{entity}"


def ensure_bronze_table(spark: SparkSession, entity: SourceEntity) -> str:
    identifier = bronze_table(entity.name)
    columns = [f"`{c}` STRING" for c in entity.columns]
    columns += [f"{name} {dtype}" for name, dtype in METADATA_COLUMNS]
    created = create_table(
        spark,
        identifier,
        ", ".join(columns),
        partitioned_by=["_batch_id"],
        properties={"icebergflow.layer": "bronze"},
    )
    if created:
        logger.info("created table", extra={"table": identifier})
    return identifier


def committed_batches(spark: SparkSession, identifier: str) -> set[str]:
    """Batch ids recorded on snapshots that are ancestors of the current table state."""
    rows = spark.sql(
        f"SELECT s.summary['{BATCH_PROPERTY}'] AS batch_id "
        f"FROM {identifier}.snapshots s JOIN {identifier}.history h "
        f"ON s.snapshot_id = h.snapshot_id "
        f"WHERE h.is_current_ancestor AND s.summary['{BATCH_PROPERTY}'] IS NOT NULL"
    ).collect()
    return {row.batch_id for row in rows}


def read_source(spark: SparkSession, batch: SourceBatch, entity: SourceEntity) -> DataFrame:
    path = batch.file(entity)
    if not path.is_file():
        raise IngestionError(f"missing source file {batch.batch_id}/{entity.filename}")
    reader = spark.read
    if entity.file_format == "csv":
        reader = reader.option("header", "true").option("inferSchema", "false")
        return reader.csv(str(path))
    return (
        reader.option("primitivesAsString", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(str(path))
    )


def evolve_schema(
    spark: SparkSession, identifier: str, incoming: list[str], source_columns: list[str]
) -> list[str]:
    """Add incoming columns that the table does not have yet; returns the added names."""
    new = [c for c in incoming if c not in source_columns and c != "_corrupt_record"]
    invalid = sorted(c for c in new if not VALID_COLUMN_NAME.match(c))
    if invalid:
        raise SchemaDriftError(f"invalid source column names: {', '.join(invalid)}")
    after = source_columns[-1]
    for column in new:
        spark.sql(f"ALTER TABLE {identifier} ADD COLUMN `{column}` STRING AFTER `{after}`")
        after = column
    if new:
        logger.info("evolved table schema", extra={"table": identifier, "added": ",".join(new)})
    return new


def align_to_table(df: DataFrame, table_columns: list[str]) -> DataFrame:
    """Project ``df`` onto the table's source columns, filling absent ones with NULL."""
    from pyspark.sql import functions as F

    incoming = [c for c in df.columns if c != "_corrupt_record"]
    unexpected = sorted(set(incoming) - set(table_columns))
    if unexpected:
        raise SchemaDriftError(f"unexpected source columns: {', '.join(unexpected)}")
    projected = [
        (F.col(f"`{c}`") if c in incoming else F.lit(None)).cast("string").alias(c)
        for c in table_columns
    ]
    corrupt = F.col("_corrupt_record") if "_corrupt_record" in df.columns else F.lit(None)
    return df.select(*projected, corrupt.cast("string").alias("_corrupt_record"))


def with_lineage(df: DataFrame, batch: SourceBatch, entity: SourceEntity) -> DataFrame:
    from pyspark.sql import functions as F

    source_columns = [c for c in df.columns if c != "_corrupt_record"]
    return df.select(
        *[F.col(f"`{c}`") for c in source_columns],
        F.lit(batch.batch_id).alias("_batch_id"),
        F.lit(f"{batch.batch_id}/{entity.filename}").alias("_source_file"),
        F.current_timestamp().alias("_ingested_at"),
        F.sha2(F.to_json(F.struct(*[F.col(f"`{c}`") for c in source_columns])), 256).alias(
            "_record_hash"
        ),
        F.col("_corrupt_record"),
    )


def ingest_entity(
    spark: SparkSession,
    batch: SourceBatch,
    entity: SourceEntity,
    already_committed: set[str] | None = None,
) -> IngestionResult:
    identifier = ensure_bronze_table(spark, entity)
    committed = (
        committed_batches(spark, identifier) if already_committed is None else already_committed
    )
    if batch.batch_id in committed:
        logger.info(
            "batch already ingested, skipping",
            extra={"table": identifier, "batch_id": batch.batch_id},
        )
        return IngestionResult(batch.batch_id, entity.name, "skipped", 0)

    raw = read_source(spark, batch, entity)
    source_columns = [c for c in spark.table(identifier).columns if not c.startswith("_")]
    if evolve_schema(spark, identifier, raw.columns, source_columns):
        spark.catalog.refreshTable(identifier)
    table_columns = spark.table(identifier).columns
    source_columns = [c for c in table_columns if not c.startswith("_")]
    df = with_lineage(align_to_table(raw, source_columns), batch, entity).select(*table_columns)

    df = df.cache()
    rows = df.count()
    expected = batch.expected_rows(entity)
    if expected is not None and rows != expected:
        df.unpersist()
        raise IngestionError(
            f"{batch.batch_id}/{entity.filename}: read {rows} rows, manifest declares {expected}"
        )

    (
        df.writeTo(identifier)
        .option(f"snapshot-property.{BATCH_PROPERTY}", batch.batch_id)
        .option("snapshot-property.icebergflow.source-rows", str(rows))
        .append()
    )
    df.unpersist()
    logger.info(
        "ingested batch", extra={"table": identifier, "batch_id": batch.batch_id, "rows": rows}
    )
    return IngestionResult(batch.batch_id, entity.name, "ingested", rows)


def ingest(
    spark: SparkSession,
    settings: Settings,
    batch_ids: list[str] | None = None,
    entities: list[str] | None = None,
) -> list[IngestionResult]:
    selected = [ENTITIES[name] for name in (entities or list(ENTITIES))]
    batches = discover_batches(settings.raw_dir, batch_ids)
    results = []
    for entity in selected:
        identifier = ensure_bronze_table(spark, entity)
        committed = committed_batches(spark, identifier)
        for batch in batches:
            result = ingest_entity(spark, batch, entity, committed)
            committed.add(batch.batch_id)
            results.append(result)
    return results
