"""Reproducible IcebergFlow benchmarks.

1. Table layout: identical synthetic order lines written three ways
   (unpartitioned in shuffled arrival order, unpartitioned sorted by time,
   partitioned by ``days(order_ts)``) and queried with selective and full-scan
   aggregations. Besides wall time, Iceberg scan metrics record how many data
   files and bytes each query actually planned to read.
2. Processing strategy: bringing Silver up to date after one new daily batch,
   incrementally (snapshot-range read from the watermark) versus a full
   refresh of all Bronze history. Before every trial, Silver and the watermark
   table are rolled back to the same snapshot, so each trial starts from an
   identical state.

Each figure is the median of ``--repeat`` timed runs after one untimed warm-up.
The script runs in a fresh temporary warehouse and writes results plus the
execution environment to ``benchmarks/results/latest.json``.

Usage: python benchmarks/run_benchmarks.py [--rows N] [--days N] [--batches N] [--repeat N]
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import platform
import statistics
import tempfile
import time
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from icebergflow.config import Settings
from icebergflow.datagen import GeneratorConfig, generate
from icebergflow.ingestion.bronze import ingest
from icebergflow.ingestion.sources import discover_batches
from icebergflow.lakehouse.catalog import create_table, current_snapshot_id
from icebergflow.lakehouse.metrics import collect_with_scan_metrics
from icebergflow.lakehouse.runtime import ICEBERG_VERSION
from icebergflow.lakehouse.spark import create_spark_session
from icebergflow.logging_config import configure_logging
from icebergflow.transformations.silver import run_silver

logger = logging.getLogger("icebergflow.benchmarks")

RESULTS_FILE = Path(__file__).parent / "results" / "latest.json"
EPOCH_2024 = 1_704_067_200

LAYOUTS: dict[str, tuple[tuple[str, ...], str]] = {
    "unpartitioned_shuffled": ((), "rand(7)"),
    "unpartitioned_sorted": ((), "order_ts"),
    "partitioned_by_day": (("days(order_ts)",), "order_ts"),
}
# Selective windows sit inside the generated range: one day at the midpoint and
# one week starting two thirds of the way in.
QUERIES = {
    "one_day_revenue": (
        "SELECT SUM(amount) FROM {t} WHERE order_ts >= date_add(date'2024-01-01', {mid}) "
        "AND order_ts < date_add(date'2024-01-01', {mid} + 1)"
    ),
    "one_week_by_category": (
        "SELECT category, SUM(amount) FROM {t} WHERE order_ts >= date_add(date'2024-01-01', "
        "{late}) AND order_ts < date_add(date'2024-01-01', {late} + 7) GROUP BY category"
    ),
    "full_scan_by_category": "SELECT category, SUM(amount) FROM {t} GROUP BY category",
}
SILVER_STATE = [
    "silver.customers",
    "silver.products",
    "silver.orders",
    "silver.order_items",
    "silver.quarantine",
    "ops.watermarks",
]


def median_seconds(fn: Callable[[], object], repeat: int) -> float:
    fn()
    timings = []
    for _ in range(repeat):
        start = time.perf_counter()
        fn()
        timings.append(time.perf_counter() - start)
    return round(statistics.median(timings), 3)


def synthetic_lines(spark: SparkSession, rows: int, days: int):
    return spark.range(rows).selectExpr(
        "id AS line_id",
        f"timestamp_seconds({EPOCH_2024} + pmod(xxhash64(id, 1), {days * 86_400})) AS order_ts",
        "element_at(array('Books', 'Electronics', 'Grocery', 'Clothing', 'Beauty', 'Toys', "
        "'Sports', 'Home'), CAST(1 + pmod(xxhash64(id, 2), 8) AS INT)) AS category",
        "CAST((100 + pmod(xxhash64(id, 3), 20000)) / 100 AS DECIMAL(10,2)) AS amount",
    )


def layout_benchmark(spark: SparkSession, rows: int, days: int, repeat: int) -> list[dict]:
    source = synthetic_lines(spark, rows, days).cache()
    source.count()
    results = []
    for layout, (partitioning, order) in LAYOUTS.items():
        table = f"bench.{layout}"
        create_table(
            spark,
            table,
            "line_id BIGINT, order_ts TIMESTAMP, category STRING, amount DECIMAL(10,2)",
            partitioned_by=partitioning,
        )
        start = time.perf_counter()
        source.orderBy(F.expr(order)).writeTo(table).overwrite(F.lit(True))
        write_seconds = round(time.perf_counter() - start, 3)
        files, size = spark.sql(
            f"SELECT COUNT(*), SUM(file_size_in_bytes) FROM {table}.files"
        ).first()

        for query, template in QUERIES.items():
            sql = template.format(t=table, mid=days // 2, late=days * 2 // 3)
            seconds = median_seconds(lambda s=sql: spark.sql(s).collect(), repeat)
            _, metrics = collect_with_scan_metrics(spark.sql(sql))
            results.append(
                {
                    "layout": layout,
                    "query": query,
                    "median_seconds": seconds,
                    "data_files_scanned": metrics.get("resultDataFiles"),
                    "data_files_skipped": metrics.get("skippedDataFiles"),
                    "data_files_total": files,
                    "bytes_scanned": metrics.get("totalDataFileSize"),
                    "table_bytes": size,
                    "write_seconds": write_seconds,
                }
            )
            logger.info("layout measured", extra=results[-1])
    source.unpersist()
    return results


def processing_benchmark(
    spark: SparkSession, settings: Settings, batches: int, repeat: int
) -> list[dict]:
    generate(
        GeneratorConfig(days=batches, orders_per_day=400, initial_customers=600, seed=42),
        settings.raw_dir,
    )
    batch_ids = [b.batch_id for b in discover_batches(settings.raw_dir)]
    ingest(spark, settings, batch_ids[:-1])
    run_silver(spark)
    baseline = {t: s for t in SILVER_STATE if (s := current_snapshot_id(spark, t)) is not None}
    ingest(spark, settings, batch_ids[-1:])
    new_rows = sum(
        spark.table(f"bronze.{e}").where(f"_batch_id = '{batch_ids[-1]}'").count()
        for e in ("customers", "products", "orders", "order_items")
    )
    total_rows = sum(
        spark.table(f"bronze.{e}").count()
        for e in ("customers", "products", "orders", "order_items")
    )

    def restore() -> None:
        for table, snapshot in baseline.items():
            spark.sql(f"CALL {settings.catalog}.system.rollback_to_snapshot('{table}', {snapshot})")

    results = []
    for mode in ("incremental", "full_refresh"):
        timings, rows_read = [], 0
        for trial in range(repeat + 1):
            restore()
            start = time.perf_counter()
            outcome = run_silver(spark, full_refresh=mode == "full_refresh")
            if trial:
                timings.append(time.perf_counter() - start)
            rows_read = sum(r.input_rows for r in outcome)
        results.append(
            {
                "mode": mode,
                "median_seconds": round(statistics.median(timings), 3),
                "bronze_rows_read": rows_read,
                "bronze_rows_total": total_rows,
                "new_batch_rows": new_rows,
                "bronze_batches": len(batch_ids),
            }
        )
        logger.info("processing measured", extra=results[-1])
    return results


def environment(spark: SparkSession, settings: Settings) -> dict:
    jvm = spark.sparkContext._jvm
    return {
        "measured_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "os": platform.platform(),
        "cpu": platform.processor(),
        "logical_cpus": os.cpu_count(),
        "python": platform.python_version(),
        "pyspark": pyspark.__version__,
        "iceberg": ICEBERG_VERSION,
        "java": jvm.java.lang.System.getProperty("java.version"),
        "spark_master": settings.spark_master,
        "driver_memory": settings.driver_memory,
        "shuffle_partitions": settings.shuffle_partitions,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--rows", type=int, default=3_000_000)
    parser.add_argument("--days", type=int, default=182)
    parser.add_argument("--batches", type=int, default=30)
    parser.add_argument("--repeat", type=int, default=5)
    parser.add_argument("--output", type=Path, default=RESULTS_FILE)
    args = parser.parse_args()
    if args.days < 14:
        parser.error("--days must be at least 14 so the one-week window fits")

    base = Settings.from_env()
    configure_logging(base.log_level, base.log_format)
    with tempfile.TemporaryDirectory(
        prefix="icebergflow-bench-", ignore_cleanup_errors=True
    ) as tmp:
        settings = replace(
            base,
            warehouse_dir=Path(tmp) / "warehouse",
            raw_dir=Path(tmp) / "raw",
            jars_dir=base.jars_dir.resolve(),
            shuffle_partitions=max(base.shuffle_partitions, 16),
        )
        spark = create_spark_session(settings, "icebergflow-benchmarks")
        report = {
            "environment": environment(spark, settings),
            "parameters": {
                "rows": args.rows,
                "days": args.days,
                "batches": args.batches,
                "repeat": args.repeat,
            },
            "layout": layout_benchmark(spark, args.rows, args.days, args.repeat),
            "processing": processing_benchmark(spark, settings, args.batches, args.repeat),
        }
        spark.stop()

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    logger.info("benchmark report written", extra={"path": str(args.output)})


if __name__ == "__main__":
    main()
