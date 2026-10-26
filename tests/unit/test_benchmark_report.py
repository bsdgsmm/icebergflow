import importlib.util
from pathlib import Path

REPORT = Path(__file__).resolve().parents[2] / "benchmarks" / "report.py"
spec = importlib.util.spec_from_file_location("benchmark_report", REPORT)
report = importlib.util.module_from_spec(spec)
spec.loader.exec_module(report)

SAMPLE = {
    "environment": {
        "measured_at": "2026-01-01T00:00:00+00:00",
        "os": "Linux",
        "logical_cpus": 4,
        "python": "3.12.0",
        "pyspark": "4.0.4",
        "iceberg": "1.10.1",
        "java": "17",
        "spark_master": "local[*]",
        "driver_memory": "2g",
        "shuffle_partitions": 16,
    },
    "parameters": {"rows": 1000, "days": 30, "batches": 5, "repeat": 3},
    "layout": [
        {
            "layout": "partitioned_by_day",
            "query": "one_day_revenue",
            "median_seconds": 0.1234,
            "data_files_scanned": 1,
            "data_files_total": 30,
            "bytes_scanned": 3 * 1024 * 1024,
        }
    ],
    "processing": [
        {
            "mode": "incremental",
            "median_seconds": 2.5,
            "bronze_rows_read": 1200,
            "bronze_rows_total": 6000,
        }
    ],
}


def test_markdown_renders_measured_values_only():
    text = report.markdown(SAMPLE)
    assert "| partitioned_by_day | one_day_revenue | 0.123 | 1 / 30 | 3.0 |" in text
    assert "| incremental | 2.500 | 1,200 | 6,000 |" in text
    assert "Median of 3 runs" in text
    assert "(1,000 rows over 30 days)" in text
