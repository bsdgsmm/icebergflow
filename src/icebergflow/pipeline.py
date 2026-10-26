"""End-to-end run: Bronze ingestion, incremental Silver, quality gates, Gold refresh.

The Silver check suite runs before Gold is rebuilt: if an error-level check
fails, the run stops and the previously published Gold tables stay untouched.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime
from typing import TYPE_CHECKING

from icebergflow.ingestion.bronze import IngestionResult, ingest
from icebergflow.quality.suite import CheckResult, run_suite
from icebergflow.transformations.gold import GoldResult, run_gold
from icebergflow.transformations.silver import SilverResult, run_silver

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from icebergflow.config import Settings

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PipelineResult:
    bronze: list[IngestionResult]
    silver: list[SilverResult]
    gold: list[GoldResult]
    affected_months: list[date] | None
    quality: list[CheckResult]


def affected_months(spark: SparkSession, since: datetime) -> list[date]:
    """Order months whose Silver orders or order lines changed at or after ``since``."""
    rows = spark.sql(
        "SELECT trunc(o.order_date, 'month') AS month FROM silver.orders o "
        "WHERE o._processed_at >= :since "
        "UNION "
        "SELECT trunc(o.order_date, 'month') AS month FROM silver.order_items i "
        "JOIN silver.orders o ON o.order_id = i.order_id WHERE i._processed_at >= :since",
        args={"since": since},
    ).collect()
    return sorted(row.month for row in rows if row.month is not None)


def run_pipeline(
    spark: SparkSession,
    settings: Settings,
    batch_ids: list[str] | None = None,
    full_refresh: bool = False,
    quality_checks: bool = True,
) -> PipelineResult:
    bronze = ingest(spark, settings, batch_ids)

    started_at = spark.sql("SELECT current_timestamp() AS ts").first().ts
    silver = run_silver(spark, full_refresh=full_refresh)
    quality = run_suite(spark, "silver") if quality_checks else []

    months = None if full_refresh else affected_months(spark, started_at)
    gold = run_gold(spark, settings.sql_dir, months=months)
    if quality_checks:
        quality += run_suite(spark, "gold")

    logger.info(
        "pipeline finished",
        extra={
            "bronze_batches_ingested": sum(r.status == "ingested" for r in bronze),
            "silver_rows_merged": sum(r.merged_rows for r in silver),
            "silver_modes": ",".join(sorted({r.mode for r in silver})),
            "affected_months": "all" if months is None else len(months),
        },
    )
    return PipelineResult(bronze, silver, gold, months, quality)
