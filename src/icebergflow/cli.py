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
        f"summary['added-records'] AS added_records, "
        f"summary['total-records'] AS total_records "
        f"FROM {args.table}.snapshots ORDER BY committed_at"
    ).show(args.limit, truncate=False)
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
