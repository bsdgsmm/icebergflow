"""SparkSession factory wired to a local Iceberg catalog."""

from __future__ import annotations

import os
import sys
from importlib import resources
from pathlib import Path
from typing import TYPE_CHECKING

from icebergflow.config import Settings
from icebergflow.lakehouse.runtime import (
    LOCALFS_CLASS,
    is_windows,
    missing_jars,
    required_jars,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def spark_conf(settings: Settings, windows: bool | None = None) -> dict[str, str]:
    windows = is_windows() if windows is None else windows
    catalog = f"spark.sql.catalog.{settings.catalog}"
    log4j = resources.files("icebergflow.lakehouse") / "resources" / "log4j2.properties"
    log4j_uri = Path(str(log4j)).resolve().as_uri()
    classpath = os.pathsep.join(
        str(jar.resolve()) for jar in required_jars(settings.jars_dir, windows)
    )
    conf = {
        "spark.master": settings.spark_master,
        "spark.driver.memory": settings.driver_memory,
        "spark.driver.extraClassPath": classpath,
        "spark.driver.extraJavaOptions": f"-Dlog4j2.configurationFile={log4j_uri}",
        "spark.sql.extensions": (
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        ),
        catalog: "org.apache.iceberg.spark.SparkCatalog",
        f"{catalog}.type": "hadoop",
        f"{catalog}.warehouse": settings.warehouse_dir.resolve().as_uri(),
        "spark.sql.defaultCatalog": settings.catalog,
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.shuffle.partitions": str(settings.shuffle_partitions),
        "spark.ui.enabled": "false",
        "spark.ui.showConsoleProgress": "false",
    }
    if windows:
        conf["spark.hadoop.fs.file.impl"] = LOCALFS_CLASS
        conf["spark.hadoop.fs.file.impl.disable.cache"] = "true"
    return conf


def create_spark_session(settings: Settings, app_name: str = "icebergflow") -> SparkSession:
    missing = missing_jars(settings.jars_dir)
    if missing:
        names = ", ".join(path.name for path in missing)
        raise RuntimeError(f"missing runtime jars ({names}); run `icebergflow setup` first")

    from pyspark.sql import SparkSession

    # Python workers must run the same interpreter as the driver, not whatever
    # `python` resolves to on PATH (on Windows that can be the Store stub).
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    builder = SparkSession.builder.appName(app_name)
    for key, value in spark_conf(settings).items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
