from pathlib import Path

import pytest

from icebergflow.lakehouse.catalog import (
    create_table,
    current_snapshot_id,
    snapshots,
    table_exists,
)
from icebergflow.lakehouse.metrics import collect_with_scan_metrics


@pytest.fixture
def events(spark, namespace):
    identifier = f"{namespace}.events"
    create_table(
        spark,
        identifier,
        "event_id BIGINT NOT NULL, event_date DATE, amount DECIMAL(10,2)",
        partitioned_by=["days(event_date)"],
    )
    return identifier


def _insert(spark, identifier, values):
    spark.sql(f"INSERT INTO {identifier} VALUES {values}")


def test_create_table_is_idempotent(spark, events):
    assert table_exists(spark, events)
    assert create_table(spark, events, "event_id BIGINT") is False


def test_table_uses_iceberg_v2_with_parquet_data_files(spark, settings, events):
    _insert(spark, events, "(1, date'2024-01-01', 10.00), (2, date'2024-01-02', 5.50)")

    props = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {events}").collect()}
    assert props["format-version"] == "2"
    assert props["write.parquet.compression-codec"] == "zstd"

    files = spark.sql(f"SELECT file_path, file_format FROM {events}.files").collect()
    assert len(files) == 2, "one data file per day partition"
    assert {f.file_format for f in files} == {"PARQUET"}
    assert all(f.file_path.endswith(".parquet") for f in files)

    namespace, table = events.split(".")
    metadata_dir = settings.warehouse_dir / namespace / table / "metadata"
    assert (metadata_dir / "version-hint.text").is_file()
    assert any(Path(metadata_dir).glob("*.metadata.json"))


def test_each_commit_creates_a_snapshot(spark, events):
    _insert(spark, events, "(1, date'2024-01-01', 10.00)")
    _insert(spark, events, "(2, date'2024-01-02', 20.00)")

    history = snapshots(spark, events)
    assert [s.operation for s in history] == ["append", "append"]
    assert history[1].parent_id == history[0].snapshot_id
    assert history[1].summary["total-records"] == "2"
    assert current_snapshot_id(spark, events) == history[1].snapshot_id


def test_time_travel_reads_historical_state(spark, events):
    _insert(spark, events, "(1, date'2024-01-01', 10.00)")
    first = snapshots(spark, events)[0].snapshot_id
    _insert(spark, events, "(2, date'2024-01-02', 20.00)")

    assert spark.table(events).count() == 2
    historical = spark.sql(f"SELECT * FROM {events} VERSION AS OF {first}")
    assert [r.event_id for r in historical.collect()] == [1]


def test_rollback_restores_previous_snapshot(spark, settings, events):
    _insert(spark, events, "(1, date'2024-01-01', 10.00)")
    good = snapshots(spark, events)[0].snapshot_id
    _insert(spark, events, "(99, date'2024-01-03', -1.00)")

    spark.sql(f"CALL {settings.catalog}.system.rollback_to_snapshot('{events}', {good})")

    assert [r.event_id for r in spark.table(events).collect()] == [1]
    assert current_snapshot_id(spark, events) == good


def test_partition_filter_prunes_data_files(spark, events):
    _insert(
        spark,
        events,
        ", ".join(f"({i}, date'2024-01-{i:02d}', {i}.00)" for i in range(1, 11)),
    )
    total = spark.sql(f"SELECT count(*) AS n FROM {events}.files").first().n
    rows, metrics = collect_with_scan_metrics(
        spark.table(events).where("event_date = date'2024-01-05'")
    )

    assert total == 10
    assert [r.event_id for r in rows] == [5]
    assert metrics["resultDataFiles"] == 1
    assert metrics["skippedDataFiles"] == 9
