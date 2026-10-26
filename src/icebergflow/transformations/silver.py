"""Bronze-to-Silver processing: parse, validate, check references, deduplicate, merge.

Per entity, in dependency order (customers, products, orders, order_items):

1. Parse raw strings into typed columns (ANSI-safe ``try_*`` casts).
2. Evaluate validation rules and foreign keys; every failed rule adds a reason.
3. Records with at least one reason go to ``silver.quarantine`` with the raw
   record as JSON. The quarantine merge is keyed on (entity, batch, record hash),
   so reprocessing never duplicates entries.
4. Valid records are deduplicated on the business key, keeping the latest
   ``updated_at`` (ties broken by batch id, then record hash, for determinism).
5. ``MERGE INTO`` the Silver table. A matched row is only overwritten by a
   strictly newer version, so late re-deliveries of old events cannot roll
   state back and replaying the same input is a no-op.

Incremental reads: each entity keeps a watermark (the last Bronze snapshot it
processed) in ``ops.watermarks``. A run reads only rows appended between the
watermark and the current Bronze snapshot via an Iceberg incremental append
scan. If the watermark is missing, or Bronze no longer descends from it through
appends only (rollback, delete, overwrite), the entity falls back to a full read
of the current snapshot. Because steps 3 and 5 are idempotent, a crash between
the merge and the watermark update only causes the same range to be re-read.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

from pyspark.sql import Window
from pyspark.sql import functions as F

from icebergflow.lakehouse.catalog import appended_since, create_table, current_snapshot_id
from icebergflow.lakehouse.watermarks import get_watermark, set_watermark
from icebergflow.transformations.silver_specs import (
    QUARANTINE_COLUMNS,
    QUARANTINE_TABLE,
    SPECS,
    SilverSpec,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)

ENTITY_ORDER = ("customers", "products", "orders", "order_items")
LINEAGE_COLUMNS = ("_batch_id", "_record_hash", "_corrupt_record")


@dataclass(frozen=True)
class SilverResult:
    entity: str
    input_rows: int
    valid_rows: int
    quarantined_rows: int
    merged_rows: int
    mode: str = "full"
    source_snapshot_id: int | None = None

    @property
    def duplicates_removed(self) -> int:
        return self.valid_rows - self.merged_rows


def ensure_silver_tables(spark: SparkSession) -> None:
    for spec in SPECS.values():
        create_table(
            spark,
            spec.table,
            spec.columns,
            partitioned_by=spec.partitioned_by,
            properties={"icebergflow.layer": "silver"},
        )
    create_table(
        spark,
        QUARANTINE_TABLE,
        QUARANTINE_COLUMNS,
        partitioned_by=["entity"],
        properties={"icebergflow.layer": "silver"},
    )


def parse(bronze: DataFrame, spec: SilverSpec) -> DataFrame:
    source_columns = [c for c in bronze.columns if not c.startswith("_")]
    return bronze.select(
        *spec.parse(),
        F.struct(*[F.col(f"`{c}`") for c in source_columns]).alias("_raw"),
        *LINEAGE_COLUMNS,
    )


def validate(spark: SparkSession, parsed: DataFrame, spec: SilverSpec) -> DataFrame:
    """Add ``_reasons`` (array of failed rule names; empty means valid)."""
    checks = [F.when(condition, F.lit(reason)) for condition, reason in spec.rules()]
    checks.append(F.when(F.col("_corrupt_record").isNotNull(), F.lit("corrupt_record")))

    df = parsed
    for index, ref in enumerate(spec.references):
        alias = f"_ref_{index}"
        keys = spark.table(ref.table).select(F.col(ref.target_column).alias(alias)).distinct()
        df = df.join(keys, df[ref.column] == keys[alias], "left")
        checks.append(
            F.when(F.col(ref.column).isNotNull() & F.col(alias).isNull(), F.lit(ref.reason))
        )

    ref_aliases = [f"_ref_{i}" for i in range(len(spec.references))]
    return df.withColumn("_reasons", F.array_compact(F.array(*checks))).drop(*ref_aliases)


def deduplicate(valid: DataFrame, spec: SilverSpec) -> DataFrame:
    ordering = [F.col("_batch_id").desc(), F.col("_record_hash").desc()]
    if spec.version_column:
        ordering.insert(0, F.col(spec.version_column).desc_nulls_last())
    window = Window.partitionBy(*spec.keys).orderBy(*ordering)
    return valid.withColumn("_rank", F.row_number().over(window)).where("_rank = 1").drop("_rank")


def _merge(spark: SparkSession, source: DataFrame, spec: SilverSpec) -> None:
    view = f"_silver_source_{spec.entity}"
    source.createOrReplaceTempView(view)
    on = " AND ".join(f"t.{k} = s.{k}" for k in spec.keys)
    matched = (
        f"WHEN MATCHED AND s.{spec.version_column} > t.{spec.version_column} THEN UPDATE SET *"
        if spec.version_column
        else ""
    )
    spark.sql(
        f"MERGE INTO {spec.table} t USING {view} s ON {on} {matched} WHEN NOT MATCHED THEN INSERT *"
    )
    spark.catalog.dropTempView(view)


def _quarantine(spark: SparkSession, invalid: DataFrame, spec: SilverSpec) -> None:
    view = f"_quarantine_source_{spec.entity}"
    invalid.select(
        F.lit(spec.entity).alias("entity"),
        F.col("_batch_id").alias("batch_id"),
        F.col("_record_hash").alias("record_hash"),
        F.col("_reasons").alias("reasons"),
        F.coalesce(F.col("_corrupt_record"), F.to_json("_raw")).alias("record"),
        F.current_timestamp().alias("quarantined_at"),
    ).dropDuplicates(["batch_id", "record_hash"]).createOrReplaceTempView(view)
    spark.sql(
        f"MERGE INTO {QUARANTINE_TABLE} t USING {view} s "
        f"ON t.entity = s.entity AND t.batch_id = s.batch_id AND t.record_hash = s.record_hash "
        f"WHEN NOT MATCHED THEN INSERT *"
    )
    spark.catalog.dropTempView(view)


def process_entity(spark: SparkSession, bronze: DataFrame, spec: SilverSpec) -> SilverResult:
    checked = validate(spark, parse(bronze, spec), spec).cache()
    try:
        valid = checked.where(F.size("_reasons") == 0)
        invalid = checked.where(F.size("_reasons") > 0)
        latest = deduplicate(valid, spec).select(
            *[c for c in spark.table(spec.table).columns if not c.startswith("_")],
            F.col("_batch_id"),
            F.current_timestamp().alias("_processed_at"),
        )
        result = SilverResult(
            entity=spec.entity,
            input_rows=checked.count(),
            valid_rows=valid.count(),
            quarantined_rows=invalid.count(),
            merged_rows=latest.count(),
        )
        if result.quarantined_rows:
            _quarantine(spark, invalid, spec)
        if result.merged_rows:
            _merge(spark, latest, spec)
    finally:
        checked.unpersist()
    logger.info(
        "silver entity processed",
        extra={
            "table": spec.table,
            "input": result.input_rows,
            "valid": result.valid_rows,
            "quarantined": result.quarantined_rows,
            "merged": result.merged_rows,
        },
    )
    return result


@dataclass(frozen=True)
class BronzeChanges:
    df: DataFrame | None
    snapshot_id: int | None
    mode: str


def bronze_changes(spark: SparkSession, entity: str, full_refresh: bool = False) -> BronzeChanges:
    """Rows of ``bronze.<entity>`` not yet processed into Silver."""
    table = f"bronze.{entity}"
    current = current_snapshot_id(spark, table)
    if current is None:
        return BronzeChanges(None, None, "empty")
    last = None if full_refresh else get_watermark(spark, SPECS[entity].table, table)
    if last == current:
        return BronzeChanges(None, current, "up_to_date")

    catalog = spark.conf.get("spark.sql.defaultCatalog")
    reader = spark.read.format("iceberg")
    if last is not None and appended_since(spark, table, last):
        reader = reader.option("start-snapshot-id", str(last)).option(
            "end-snapshot-id", str(current)
        )
        mode = "incremental"
    else:
        reader = reader.option("snapshot-id", str(current))
        mode = "full"
    return BronzeChanges(reader.load(f"{catalog}.{table}"), current, mode)


def run_silver(
    spark: SparkSession, entities: list[str] | None = None, full_refresh: bool = False
) -> list[SilverResult]:
    """Process unprocessed Bronze rows (or all of them with ``full_refresh``) into Silver."""
    ensure_silver_tables(spark)
    results = []
    for entity in entities or ENTITY_ORDER:
        spec = SPECS[entity]
        changes = bronze_changes(spark, entity, full_refresh)
        if changes.df is None:
            logger.info("silver entity up to date", extra={"table": spec.table})
            results.append(SilverResult(entity, 0, 0, 0, 0, changes.mode, changes.snapshot_id))
            continue
        result = process_entity(spark, changes.df, spec)
        set_watermark(spark, spec.table, f"bronze.{entity}", changes.snapshot_id)
        results.append(replace(result, mode=changes.mode, source_snapshot_id=changes.snapshot_id))
    return results
