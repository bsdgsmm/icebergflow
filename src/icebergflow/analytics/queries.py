"""Catalog and runner for the analytical queries in ``sql/analytics``.

Each query file starts with a ``-- Question:`` line stating the business
question it answers; that line doubles as the description shown by
``icebergflow query --list``.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

QUESTION_PREFIX = "-- Question:"


@dataclass(frozen=True)
class AnalyticalQuery:
    name: str
    question: str
    path: Path

    @property
    def sql(self) -> str:
        return self.path.read_text(encoding="utf-8")


def list_queries(sql_dir: Path) -> list[AnalyticalQuery]:
    queries = []
    for path in sorted((sql_dir / "analytics").glob("*.sql")):
        first_line = path.read_text(encoding="utf-8").splitlines()[0]
        if not first_line.startswith(QUESTION_PREFIX):
            raise ValueError(f"{path.name} must start with '{QUESTION_PREFIX}'")
        queries.append(
            AnalyticalQuery(path.stem, first_line.removeprefix(QUESTION_PREFIX).strip(), path)
        )
    return queries


def get_query(sql_dir: Path, name: str) -> AnalyticalQuery:
    queries = {q.name: q for q in list_queries(sql_dir)}
    if name not in queries:
        raise KeyError(f"unknown query '{name}'; available: {', '.join(sorted(queries))}")
    return queries[name]


def run_query(spark: SparkSession, sql_dir: Path, name: str) -> DataFrame:
    return spark.sql(get_query(sql_dir, name).sql)
