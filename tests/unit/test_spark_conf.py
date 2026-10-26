import os
from pathlib import Path

from icebergflow.config import Settings
from icebergflow.lakehouse.runtime import (
    ICEBERG_RUNTIME,
    LOCALFS_CLASS,
    LOCALFS_JAR,
    missing_jars,
    required_jars,
)
from icebergflow.lakehouse.spark import spark_conf


def test_iceberg_runtime_matches_spark_and_scala_binary_versions():
    assert ICEBERG_RUNTIME.artifact == "iceberg-spark-runtime-4.0_2.13"
    assert ICEBERG_RUNTIME.url == (
        "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.0_2.13/"
        f"{ICEBERG_RUNTIME.version}/iceberg-spark-runtime-4.0_2.13-{ICEBERG_RUNTIME.version}.jar"
    )


def test_windows_requires_local_file_system_shim(tmp_path):
    assert [p.name for p in required_jars(tmp_path, windows=False)] == [ICEBERG_RUNTIME.filename]
    assert [p.name for p in required_jars(tmp_path, windows=True)] == [
        ICEBERG_RUNTIME.filename,
        LOCALFS_JAR,
    ]


def test_missing_jars_reports_absent_files(tmp_path):
    (tmp_path / ICEBERG_RUNTIME.filename).write_bytes(b"")
    assert missing_jars(tmp_path, windows=False) == []
    assert missing_jars(tmp_path, windows=True) == [tmp_path / LOCALFS_JAR]


def test_catalog_configuration(tmp_path):
    settings = Settings(warehouse_dir=tmp_path / "wh", jars_dir=tmp_path, catalog="lake")
    conf = spark_conf(settings, windows=False)

    assert conf["spark.sql.catalog.lake"] == "org.apache.iceberg.spark.SparkCatalog"
    assert conf["spark.sql.catalog.lake.type"] == "hadoop"
    assert conf["spark.sql.catalog.lake.warehouse"] == (tmp_path / "wh").resolve().as_uri()
    assert conf["spark.sql.defaultCatalog"] == "lake"
    assert conf["spark.sql.session.timeZone"] == "UTC"
    assert "IcebergSparkSessionExtensions" in conf["spark.sql.extensions"]
    assert "spark.hadoop.fs.file.impl" not in conf
    assert Path(conf["spark.driver.extraClassPath"]).name == ICEBERG_RUNTIME.filename


def test_windows_configuration_registers_shim(tmp_path):
    conf = spark_conf(Settings(jars_dir=tmp_path), windows=True)
    assert conf["spark.hadoop.fs.file.impl"] == LOCALFS_CLASS
    assert conf["spark.driver.extraClassPath"].split(os.pathsep)[1].endswith(LOCALFS_JAR)
