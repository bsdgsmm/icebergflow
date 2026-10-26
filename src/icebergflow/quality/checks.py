"""Check definitions. Every check compiles to one SQL query returning a violation count."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

ERROR = "error"
WARN = "warn"


@dataclass(frozen=True)
class Check:
    name: str
    table: str
    sql: str
    severity: str = ERROR
    description: str = ""


def _quote(values: Sequence[str]) -> str:
    return ", ".join("'" + v.replace("'", "''") + "'" for v in values)


def unique(table: str, keys: Sequence[str], severity: str = ERROR) -> Check:
    cols = ", ".join(keys)
    return Check(
        name=f"unique:{table}({cols})",
        table=table,
        sql=(
            f"SELECT COUNT(*) FROM (SELECT {cols} FROM {table} GROUP BY {cols} HAVING COUNT(*) > 1)"
        ),
        severity=severity,
        description=f"{cols} identifies at most one row",
    )


def not_null(table: str, columns: Sequence[str], severity: str = ERROR) -> list[Check]:
    return [
        Check(
            name=f"not_null:{table}.{column}",
            table=table,
            sql=f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL",
            severity=severity,
            description=f"{column} is always populated",
        )
        for column in columns
    ]


def accepted_values(
    table: str,
    column: str,
    values: Sequence[str],
    allow_null: bool = False,
    severity: str = ERROR,
) -> Check:
    null_clause = "" if allow_null else f" OR {column} IS NULL"
    return Check(
        name=f"accepted_values:{table}.{column}",
        table=table,
        sql=f"SELECT COUNT(*) FROM {table} WHERE {column} NOT IN ({_quote(values)}){null_clause}",
        severity=severity,
        description=f"{column} in ({', '.join(values)})",
    )


def relationship(
    table: str, column: str, ref_table: str, ref_column: str, severity: str = ERROR
) -> Check:
    return Check(
        name=f"relationship:{table}.{column}->{ref_table}.{ref_column}",
        table=table,
        sql=(
            f"SELECT COUNT(*) FROM {table} c LEFT ANTI JOIN {ref_table} p "
            f"ON c.{column} = p.{ref_column} WHERE c.{column} IS NOT NULL"
        ),
        severity=severity,
        description=f"every {column} exists in {ref_table}.{ref_column}",
    )


def expression(table: str, condition: str, name: str, severity: str = ERROR) -> Check:
    """Rows must satisfy ``condition``; NULL results count as violations."""
    return Check(
        name=f"expression:{table}:{name}",
        table=table,
        sql=f"SELECT COUNT(*) FROM {table} WHERE NOT COALESCE({condition}, FALSE)",
        severity=severity,
        description=condition,
    )


def equal(name: str, table: str, left_sql: str, right_sql: str, severity: str = ERROR) -> Check:
    """Two scalar queries must return the same value (cross-table reconciliation)."""
    return Check(
        name=f"reconcile:{name}",
        table=table,
        sql=(
            f"SELECT CASE WHEN l.v <=> r.v THEN 0 ELSE 1 END "
            f"FROM ({left_sql}) l(v) CROSS JOIN ({right_sql}) r(v)"
        ),
        severity=severity,
        description=f"({left_sql}) = ({right_sql})",
    )
