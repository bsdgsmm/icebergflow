import json
import logging

from icebergflow.logging_config import JsonFormatter, KeyValueFormatter, configure_logging


def _record(**extra):
    record = logging.makeLogRecord(
        {"name": "icebergflow.test", "levelname": "INFO", "levelno": 20, "msg": "batch loaded"}
    )
    for key, value in extra.items():
        setattr(record, key, value)
    return record


def test_json_formatter_includes_extra_fields():
    payload = json.loads(JsonFormatter().format(_record(batch_id="2024-01-01", rows=42)))
    assert payload["message"] == "batch loaded"
    assert payload["level"] == "INFO"
    assert payload["batch_id"] == "2024-01-01"
    assert payload["rows"] == 42


def test_key_value_formatter_appends_sorted_extras():
    line = KeyValueFormatter().format(_record(rows=3, batch_id="b1"))
    assert line.endswith("batch loaded batch_id=b1 rows=3")


def test_configure_logging_replaces_handlers():
    configure_logging("WARNING", "json")
    root = logging.getLogger()
    assert root.level == logging.WARNING
    assert len(root.handlers) == 1
    assert isinstance(root.handlers[0].formatter, JsonFormatter)
