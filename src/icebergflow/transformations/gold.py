"""Silver-to-Gold: analytics-ready tables defined as SQL models under ``sql/gold``.

Each model is a single SELECT statement. The table is created from the query's
schema on first run and fully overwritten afterwards. An overwrite is one
Iceberg snapshot, so readers see either the previous or the new version of a
model, never a partially written one, and earlier versions remain queryable
through time travel.

Models partitioned by month of a date column can instead be refreshed for a
set of affected months only: the query is restricted to those months and
written with a dynamic partition overwrite, which replaces exactly those
month partitions and leaves all others untouched.
"""

from __future__ import annotations

import logging
from collections.abc import Collection
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from icebergflow.lakehouse.catalog import create_table, table_exists

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class GoldModel:
    name: str
    grain: tuple[str, ...]
    partitioned_by: tuple[str, ...] = ()
    month_column: str | None = None

    @property
    def table(self) -> str:
        return f"gold.{self.name}"


# Dependency order: repeat_purchase_cohorts reads gold.customer_summary.
MODELS: tuple[GoldModel, ...] = (
    GoldModel("daily_sales", ("order_date",), ("months(order_date)",), "order_date"),
    GoldModel("product_performance", ("product_id",)),
    GoldModel("channel_daily", ("order_date", "channel"), ("months(order_date)",), "order_date"),
    GoldModel("category_monthly", ("order_month", "category")),
    GoldModel("customer_summary", ("customer_id",)),
    GoldModel("repeat_purchase_cohorts", ("cohort_month",)),
)


@dataclass(frozen=True)
class GoldResult:
    model: str
    rows: int
    mode: str = "full"


def load_sql(sql_dir: Path, model: GoldModel) -> str:
    path = sql_dir / "gold" / f"{model.name}.sql"
    if not path.is_file():
        raise FileNotFoundError(f"SQL model not found: {path}")
    return path.read_text(encoding="utf-8")


def _ddl(df: DataFrame) -> str:
    return ", ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in df.schema.fields)


def build_model(
    spark: SparkSession,
    sql_dir: Path,
    model: GoldModel,
    months: Collection[date] | None = None,
) -> GoldResult:
    """Rebuild ``model``; with ``months``, only those month partitions (if supported)."""
    df = spark.sql(load_sql(sql_dir, model))
    if not table_exists(spark, model.table):
        create_table(
            spark,
            model.table,
            _ddl(df),
            partitioned_by=model.partitioned_by,
            properties={"icebergflow.layer": "gold"},
        )
    elif model.month_column and months is not None:
        if not months:
            return GoldResult(model.name, 0, "unchanged")
        month_start = F.trunc(F.col(model.month_column), "month")
        affected = df.where(month_start.isin(*sorted(months))).cache()
        rows = affected.count()
        affected.writeTo(model.table).overwritePartitions()
        affected.unpersist()
        logger.info(
            "gold partitions refreshed",
            extra={"table": model.table, "months": len(months), "rows": rows},
        )
        return GoldResult(model.name, rows, "partitions")

    df.writeTo(model.table).overwrite(F.lit(True))
    rows = spark.table(model.table).count()
    logger.info("gold model built", extra={"table": model.table, "rows": rows})
    return GoldResult(model.name, rows)


def run_gold(
    spark: SparkSession,
    sql_dir: Path,
    models: list[str] | None = None,
    months: Collection[date] | None = None,
) -> list[GoldResult]:
    selected = [m for m in MODELS if models is None or m.name in models]
    return [build_model(spark, sql_dir, model, months) for model in selected]
