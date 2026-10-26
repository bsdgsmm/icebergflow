"""Read Iceberg scan metrics (files scanned/skipped, bytes) from executed Spark plans.

Iceberg reports per-scan counters such as ``resultDataFiles`` and
``skippedDataFiles`` as SQL metrics on Spark's ``BatchScanExec`` node. They are
the most direct evidence of partition pruning and file skipping, so tests and
benchmarks read them instead of inferring pruning from wall-clock time.
"""

from __future__ import annotations

from collections import Counter
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, Row

_SCAN_NODE = "BatchScanExec"


def _iterate(seq: Any) -> list[Any]:
    return [seq.apply(i) for i in range(seq.size())]


def _scan_nodes(node: Any) -> list[Any]:
    name = node.getClass().getSimpleName()
    if name == "AdaptiveSparkPlanExec":
        return _scan_nodes(node.executedPlan())
    if name.endswith("QueryStageExec"):
        return _scan_nodes(node.plan())
    found = [node] if name == _SCAN_NODE else []
    for child in _iterate(node.children()):
        found.extend(_scan_nodes(child))
    return found


def _metrics(node: Any) -> dict[str, int]:
    values = {}
    iterator = node.metrics().iterator()
    while iterator.hasNext():
        entry = iterator.next()
        values[entry._1()] = int(entry._2().value())
    return values


def scan_metrics(df: DataFrame) -> dict[str, int]:
    """Summed scan metrics for an already executed DataFrame."""
    totals: Counter[str] = Counter()
    for node in _scan_nodes(df._jdf.queryExecution().executedPlan()):
        totals.update(_metrics(node))
    return dict(totals)


def collect_with_scan_metrics(df: DataFrame) -> tuple[list[Row], dict[str, int]]:
    rows = df.collect()
    return rows, scan_metrics(df)
