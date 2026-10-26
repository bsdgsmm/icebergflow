"""Command-line entry point: ``icebergflow <command>``."""

from __future__ import annotations

import argparse
import logging
from collections.abc import Sequence
from datetime import date
from pathlib import Path

from icebergflow.config import Settings
from icebergflow.logging_config import configure_logging

logger = logging.getLogger("icebergflow")


def _cmd_generate(args: argparse.Namespace, settings: Settings) -> int:
    from icebergflow.datagen import GeneratorConfig, generate

    config = GeneratorConfig(
        start_date=args.start_date,
        days=args.days,
        orders_per_day=args.orders_per_day,
        initial_customers=args.customers,
        products=args.products,
        seed=args.seed,
        schema_v2_from=args.schema_v2_from,
    )
    output = args.output or settings.raw_dir
    summaries = generate(config, output)
    logger.info(
        "generation finished",
        extra={"batches": len(summaries), "output": str(output), "seed": args.seed},
    )
    return 0


def _cmd_setup(args: argparse.Namespace, settings: Settings) -> int:
    from icebergflow.lakehouse.runtime import setup_runtime

    jars = setup_runtime(settings.jars_dir)
    logger.info("runtime ready", extra={"jars": ",".join(jar.name for jar in jars)})
    return 0


def _cmd_snapshots(args: argparse.Namespace, settings: Settings) -> int:
    from icebergflow.lakehouse.spark import create_spark_session

    spark = create_spark_session(settings, "icebergflow-snapshots")
    spark.sql(
        f"SELECT committed_at, snapshot_id, parent_id, operation, "
        f"summary['icebergflow.batch-id'] AS batch_id, "
        f"summary['added-records'] AS added_records, "
        f"summary['total-records'] AS total_records "
        f"FROM {args.table}.snapshots ORDER BY committed_at"
    ).show(args.limit, truncate=False)
    return 0


def _cmd_ingest(args: argparse.Namespace, settings: Settings) -> int:
    from icebergflow.ingestion.bronze import ingest
    from icebergflow.lakehouse.spark import create_spark_session

    spark = create_spark_session(settings, "icebergflow-ingest")
    results = ingest(spark, settings, args.batch or None)
    ingested = [r for r in results if r.status == "ingested"]
    logger.info(
        "bronze ingestion finished",
        extra={
            "ingested": len(ingested),
            "skipped": len(results) - len(ingested),
            "rows": sum(r.rows for r in ingested),
        },
    )
    return 0


def _cmd_silver(args: argparse.Namespace, settings: Settings) -> int:
    from icebergflow.lakehouse.spark import create_spark_session
    from icebergflow.transformations.silver import run_silver

    spark = create_spark_session(settings, "icebergflow-silver")
    results = run_silver(spark, full_refresh=args.full_refresh)
    logger.info(
        "silver processing finished",
        extra={
            "merged": sum(r.merged_rows for r in results),
            "quarantined": sum(r.quarantined_rows for r in results),
        },
    )
    return 0


def _cmd_gold(args: argparse.Namespace, settings: Settings) -> int:
    from icebergflow.lakehouse.spark import create_spark_session
    from icebergflow.transformations.gold import run_gold

    spark = create_spark_session(settings, "icebergflow-gold")
    results = run_gold(spark, settings.sql_dir, args.model or None)
    logger.info("gold build finished", extra={"models": len(results)})
    return 0


def _cmd_run(args: argparse.Namespace, settings: Settings) -> int:
    from icebergflow.lakehouse.spark import create_spark_session
    from icebergflow.pipeline import run_pipeline

    spark = create_spark_session(settings, "icebergflow-run")
    run_pipeline(
        spark,
        settings,
        args.batch or None,
        full_refresh=args.full_refresh,
        quality_checks=not args.skip_quality,
    )
    return 0


def _cmd_quality(args: argparse.Namespace, settings: Settings) -> int:
    from icebergflow.lakehouse.spark import create_spark_session
    from icebergflow.quality.suite import ERROR, run_suite

    spark = create_spark_session(settings, "icebergflow-quality")
    suites = ["silver", "gold"] if args.suite == "all" else [args.suite]
    results = [r for suite in suites for r in run_suite(spark, suite, fail_on_error=False)]
    spark.createDataFrame(
        [(r.check, r.severity, r.failures, r.passed) for r in results],
        "check string, severity string, failures bigint, passed boolean",
    ).show(len(results), truncate=False)
    return 1 if any(not r.passed and r.severity == ERROR for r in results) else 0


def _cmd_evolve_partition(args: argparse.Namespace, settings: Settings) -> int:
    from icebergflow.lakehouse.catalog import add_partition_field, partition_fields
    from icebergflow.lakehouse.spark import create_spark_session

    spark = create_spark_session(settings, "icebergflow-maintenance")
    changed = add_partition_field(spark, args.table, args.transform)
    logger.info(
        "partition spec evolved" if changed else "partition field already present",
        extra={"table": args.table, "spec": ",".join(partition_fields(spark, args.table))},
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="icebergflow", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    gen = commands.add_parser("generate", help="write deterministic synthetic source batches")
    gen.add_argument("--output", type=Path, help="target directory (default: settings raw_dir)")
    gen.add_argument("--start-date", type=date.fromisoformat, default=date(2024, 1, 1))
    gen.add_argument("--days", type=int, default=14)
    gen.add_argument("--orders-per-day", type=int, default=150)
    gen.add_argument("--customers", type=int, default=300, help="customers on the first day")
    gen.add_argument("--products", type=int, default=80)
    gen.add_argument("--seed", type=int, default=42)
    gen.add_argument(
        "--schema-v2-from",
        type=date.fromisoformat,
        help="first batch date that carries the v2 order schema (channel, coupon_code)",
    )
    gen.set_defaults(handler=_cmd_generate)

    setup = commands.add_parser(
        "setup", help="download the Iceberg runtime jar (and build the Windows shim)"
    )
    setup.set_defaults(handler=_cmd_setup)

    ing = commands.add_parser("ingest", help="load raw source batches into Bronze tables")
    ing.add_argument("--batch", action="append", help="batch id to load (repeatable; default: all)")
    ing.set_defaults(handler=_cmd_ingest)

    silver = commands.add_parser("silver", help="validate and merge Bronze data into Silver")
    silver.add_argument(
        "--full-refresh", action="store_true", help="ignore watermarks and reprocess all of Bronze"
    )
    silver.set_defaults(handler=_cmd_silver)

    gold = commands.add_parser("gold", help="rebuild Gold models from Silver")
    gold.add_argument("--model", action="append", help="model name to build (repeatable)")
    gold.set_defaults(handler=_cmd_gold)

    run = commands.add_parser("run", help="ingest new batches, then update Silver and Gold")
    run.add_argument("--batch", action="append", help="batch id to load (repeatable)")
    run.add_argument(
        "--full-refresh", action="store_true", help="reprocess all of Bronze and rebuild Gold"
    )
    run.add_argument(
        "--skip-quality", action="store_true", help="do not run the data quality suites"
    )
    run.set_defaults(handler=_cmd_run)

    quality = commands.add_parser("quality", help="run data quality checks and show results")
    quality.add_argument("--suite", choices=["silver", "gold", "all"], default="all")
    quality.set_defaults(handler=_cmd_quality)

    evolve = commands.add_parser(
        "evolve-partition", help="add a partition field to a table (metadata-only change)"
    )
    evolve.add_argument("table", help="table identifier, e.g. silver.order_items")
    evolve.add_argument("transform", help="partition transform, e.g. 'bucket(8, order_id)'")
    evolve.set_defaults(handler=_cmd_evolve_partition)

    snaps = commands.add_parser("snapshots", help="show the snapshot history of a table")
    snaps.add_argument("table", help="table identifier, e.g. silver.orders")
    snaps.add_argument("--limit", type=int, default=50)
    snaps.set_defaults(handler=_cmd_snapshots)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings = Settings.from_env()
    configure_logging(settings.log_level, settings.log_format)
    return args.handler(args, settings)
