"""Default check suites and the runner that records results in ``ops.dq_results``."""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING

from icebergflow.lakehouse.catalog import create_table, table_exists
from icebergflow.quality.checks import (
    ERROR,
    WARN,
    Check,
    accepted_values,
    equal,
    expression,
    not_null,
    relationship,
    unique,
)
from icebergflow.transformations.silver_specs import ORDER_CHANNELS, ORDER_STATUSES

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

RESULTS_TABLE = "ops.dq_results"
NET_STATUSES = "('placed', 'shipped', 'delivered')"
MAX_QUARANTINE_SHARE = 0.10


class DataQualityError(RuntimeError):
    def __init__(self, failures: list[CheckResult]) -> None:
        self.failures = failures
        names = ", ".join(f.check for f in failures)
        super().__init__(f"{len(failures)} data quality check(s) failed: {names}")


@dataclass(frozen=True)
class CheckResult:
    check: str
    table: str
    severity: str
    failures: int

    @property
    def passed(self) -> bool:
        return self.failures == 0


def silver_checks() -> list[Check]:
    return [
        unique("silver.customers", ["customer_id"]),
        *not_null("silver.customers", ["customer_id", "email", "updated_at"]),
        expression("silver.customers", "email RLIKE '^[^@\\\\s]+@[^@\\\\s]+\\\\.[^@\\\\s]+$'",
                   "valid_email"),
        unique("silver.products", ["product_id"]),
        *not_null("silver.products", ["product_id", "category", "unit_price"]),
        expression("silver.products", "unit_price >= 0", "non_negative_price"),
        unique("silver.orders", ["order_id"]),
        *not_null("silver.orders", ["order_id", "customer_id", "order_ts", "order_date"]),
        accepted_values("silver.orders", "status", ORDER_STATUSES),
        accepted_values("silver.orders", "channel", ORDER_CHANNELS, allow_null=True),
        expression("silver.orders", "order_date = to_date(order_ts)", "order_date_matches_ts"),
        relationship("silver.orders", "customer_id", "silver.customers", "customer_id"),
        unique("silver.order_items", ["order_id", "line_number"]),
        *not_null("silver.order_items", ["order_id", "product_id", "quantity", "unit_price"]),
        expression("silver.order_items", "quantity > 0", "positive_quantity"),
        expression(
            "silver.order_items",
            "line_amount = round(quantity * unit_price * (1 - discount), 2)",
            "line_amount_formula",
        ),
        relationship("silver.order_items", "order_id", "silver.orders", "order_id"),
        relationship("silver.order_items", "product_id", "silver.products", "product_id"),
        Check(
            name="quarantine_share:silver",
            table="silver.quarantine",
            sql=(
                "SELECT COUNT(*) FROM (SELECT q.entity, COUNT(*) AS rejected FROM "
                "silver.quarantine q GROUP BY q.entity) r JOIN (SELECT 'orders' AS entity, "
                "COUNT(*) AS kept FROM silver.orders UNION ALL SELECT 'order_items', COUNT(*) "
                "FROM silver.order_items UNION ALL SELECT 'customers', COUNT(*) FROM "
                "silver.customers UNION ALL SELECT 'products', COUNT(*) FROM silver.products) k "
                f"ON r.entity = k.entity WHERE r.rejected > {MAX_QUARANTINE_SHARE} * k.kept"
            ),
            severity=WARN,
            description=f"quarantined records stay below {MAX_QUARANTINE_SHARE:.0%} per entity",
        ),
    ]  # fmt: skip


def gold_checks() -> list[Check]:
    silver_net_revenue = (
        "SELECT CAST(SUM(i.line_amount) AS DECIMAL(14,2)) FROM silver.order_items i "
        f"JOIN silver.orders o ON o.order_id = i.order_id WHERE o.status IN {NET_STATUSES}"
    )
    return [
        unique("gold.daily_sales", ["order_date"]),
        expression("gold.daily_sales", "net_revenue BETWEEN 0 AND gross_revenue",
                   "net_within_gross"),
        expression("gold.daily_sales", "net_orders + cancelled_orders + returned_orders = orders",
                   "status_counts_add_up"),
        equal("daily_sales_net_revenue", "gold.daily_sales",
              "SELECT CAST(SUM(net_revenue) AS DECIMAL(14,2)) FROM gold.daily_sales",
              silver_net_revenue),
        equal("daily_sales_orders", "gold.daily_sales",
              "SELECT SUM(orders) FROM gold.daily_sales", "SELECT COUNT(*) FROM silver.orders"),
        unique("gold.channel_daily", ["order_date", "channel"]),
        equal("channel_daily_net_revenue", "gold.channel_daily",
              "SELECT CAST(SUM(net_revenue) AS DECIMAL(14,2)) FROM gold.channel_daily",
              silver_net_revenue),
        unique("gold.product_performance", ["product_id"]),
        equal("product_performance_coverage", "gold.product_performance",
              "SELECT COUNT(*) FROM gold.product_performance",
              "SELECT COUNT(*) FROM silver.products"),
        unique("gold.category_monthly", ["order_month", "category"]),
        Check(
            name="expression:gold.category_monthly:shares_sum_to_one",
            table="gold.category_monthly",
            sql=("SELECT COUNT(*) FROM (SELECT order_month FROM gold.category_monthly "
                 "GROUP BY order_month HAVING abs(SUM(revenue_share) - 1) > 0.001)"),
            severity=WARN,
            description="monthly revenue shares sum to 1 (within rounding)",
        ),
        unique("gold.customer_summary", ["customer_id"]),
        equal("customer_summary_coverage", "gold.customer_summary",
              "SELECT COUNT(*) FROM gold.customer_summary",
              "SELECT COUNT(*) FROM silver.customers"),
        equal("customer_lifetime_revenue", "gold.customer_summary",
              "SELECT CAST(SUM(lifetime_revenue) AS DECIMAL(14,2)) FROM gold.customer_summary",
              silver_net_revenue),
        unique("gold.repeat_purchase_cohorts", ["cohort_month"]),
        expression("gold.repeat_purchase_cohorts", "repeat_rate BETWEEN 0 AND 1",
                   "repeat_rate_is_a_share"),
    ]  # fmt: skip


SUITES = {"silver": silver_checks, "gold": gold_checks}


def _ensure_results_table(spark: SparkSession) -> None:
    if not table_exists(spark, RESULTS_TABLE):
        create_table(
            spark,
            RESULTS_TABLE,
            "run_id STRING, suite STRING, check_name STRING, table_name STRING, "
            "severity STRING, failures BIGINT, passed BOOLEAN, checked_at TIMESTAMP",
            partitioned_by=["days(checked_at)"],
            properties={"icebergflow.layer": "ops"},
        )


def run_checks(
    spark: SparkSession,
    checks: list[Check],
    suite: str = "adhoc",
    fail_on_error: bool = True,
    run_id: str | None = None,
) -> list[CheckResult]:
    run_id = run_id or uuid.uuid4().hex
    results = [
        CheckResult(c.name, c.table, c.severity, int(spark.sql(c.sql).first()[0])) for c in checks
    ]

    _ensure_results_table(spark)
    rows = [(run_id, suite, r.check, r.table, r.severity, r.failures, r.passed) for r in results]
    (
        spark.createDataFrame(
            rows,
            "run_id string, suite string, check_name string, table_name string, "
            "severity string, failures bigint, passed boolean",
        )
        .selectExpr("*", "current_timestamp() AS checked_at")
        .writeTo(RESULTS_TABLE)
        .append()
    )

    failed = [r for r in results if not r.passed]
    for result in failed:
        log = logger.error if result.severity == ERROR else logger.warning
        log(
            "data quality check failed",
            extra={"check": result.check, "failures": result.failures, "severity": result.severity},
        )
    logger.info(
        "data quality suite finished",
        extra={"suite": suite, "checks": len(results), "failed": len(failed), "run_id": run_id},
    )
    errors = [r for r in failed if r.severity == ERROR]
    if errors and fail_on_error:
        raise DataQualityError(errors)
    return results


def run_suite(spark: SparkSession, suite: str, fail_on_error: bool = True) -> list[CheckResult]:
    return run_checks(spark, SUITES[suite](), suite, fail_on_error)
