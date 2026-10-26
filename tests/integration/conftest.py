"""Shared fixtures for tests that run a real local Spark session against Iceberg.

The Spark session is created once per test run with its warehouse in a
temporary directory. Each test gets an isolated namespace so tables never leak
between tests. Prerequisites: JDK 17+ (``JAVA_HOME`` or ``java`` on PATH) and
the runtime jars from ``icebergflow setup``.
"""

from __future__ import annotations

import itertools
from dataclasses import replace
from pathlib import Path

import pytest

from icebergflow.config import Settings
from icebergflow.lakehouse.runtime import missing_jars

_counter = itertools.count()


def pytest_collection_modifyitems(items):
    for item in items:
        if Path(item.fspath).parent.name == "integration":
            item.add_marker(pytest.mark.integration)


@pytest.fixture(scope="session")
def settings(tmp_path_factory) -> Settings:
    base = Settings.from_env()
    jars_dir = base.jars_dir.resolve()
    missing = missing_jars(jars_dir)
    if missing:
        pytest.fail(
            "integration tests need the runtime jars; run `icebergflow setup` "
            f"(missing: {', '.join(p.name for p in missing)})",
            pytrace=False,
        )
    root = tmp_path_factory.mktemp("lakehouse")
    return replace(
        base,
        warehouse_dir=root / "warehouse",
        raw_dir=root / "raw",
        jars_dir=jars_dir,
        spark_master="local[2]",
        shuffle_partitions=2,
    )


@pytest.fixture(scope="session")
def spark(settings):
    from icebergflow.lakehouse.spark import create_spark_session

    session = create_spark_session(settings, "icebergflow-tests")
    yield session
    session.stop()


LAYER_NAMESPACES = ("bronze", "silver", "gold", "ops")


@pytest.fixture
def lakehouse(spark, settings, tmp_path):
    """Empty medallion namespaces plus a private raw directory for one test."""
    for ns in LAYER_NAMESPACES:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ns}")
        for row in spark.sql(f"SHOW TABLES IN {ns}").collect():
            spark.sql(f"DROP TABLE {ns}.{row.tableName} PURGE")
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    return replace(settings, raw_dir=raw_dir)


@pytest.fixture
def namespace(spark) -> str:
    name = f"test_{next(_counter)}"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {name}")
    return name
