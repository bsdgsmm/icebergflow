"""Processing watermarks: the last source snapshot each consumer has fully processed."""

from __future__ import annotations

from typing import TYPE_CHECKING

from icebergflow.lakehouse.catalog import create_table

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

WATERMARK_TABLE = "ops.watermarks"


def ensure_watermark_table(spark: SparkSession) -> None:
    create_table(
        spark,
        WATERMARK_TABLE,
        "consumer STRING, source_table STRING, snapshot_id BIGINT, updated_at TIMESTAMP",
        properties={"icebergflow.layer": "ops"},
    )


def get_watermark(spark: SparkSession, consumer: str, source_table: str) -> int | None:
    ensure_watermark_table(spark)
    row = spark.sql(
        f"SELECT snapshot_id FROM {WATERMARK_TABLE} "
        "WHERE consumer = :consumer AND source_table = :source",
        args={"consumer": consumer, "source": source_table},
    ).first()
    return None if row is None else row.snapshot_id


def set_watermark(spark: SparkSession, consumer: str, source_table: str, snapshot_id: int) -> None:
    ensure_watermark_table(spark)
    spark.sql(
        f"MERGE INTO {WATERMARK_TABLE} t "
        "USING (SELECT :consumer AS consumer, :source AS source_table, "
        "CAST(:snapshot AS BIGINT) AS snapshot_id, current_timestamp() AS updated_at) s "
        "ON t.consumer = s.consumer AND t.source_table = s.source_table "
        "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *",
        args={"consumer": consumer, "source": source_table, "snapshot": snapshot_id},
    )
