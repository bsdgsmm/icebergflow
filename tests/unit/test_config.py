from pathlib import Path

import pytest

from icebergflow.config import Settings


def test_defaults_when_environment_is_empty():
    settings = Settings.from_env({})
    assert settings == Settings()
    assert settings.catalog == "lakehouse"
    assert settings.warehouse_dir == Path("warehouse")


def test_environment_overrides():
    settings = Settings.from_env(
        {
            "ICEBERGFLOW_WAREHOUSE_DIR": "/tmp/wh",
            "ICEBERGFLOW_SHUFFLE_PARTITIONS": "8",
            "ICEBERGFLOW_LOG_LEVEL": "debug",
            "ICEBERGFLOW_LOG_FORMAT": "JSON",
        }
    )
    assert settings.warehouse_dir == Path("/tmp/wh")
    assert settings.shuffle_partitions == 8
    assert settings.log_level == "DEBUG"
    assert settings.log_format == "json"


def test_blank_values_fall_back_to_defaults():
    assert Settings.from_env({"ICEBERGFLOW_CATALOG": "  "}).catalog == "lakehouse"


@pytest.mark.parametrize("value", ["0", "-1", "four"])
def test_invalid_shuffle_partitions_rejected(value):
    with pytest.raises(ValueError, match="SHUFFLE_PARTITIONS"):
        Settings.from_env({"ICEBERGFLOW_SHUFFLE_PARTITIONS": value})


def test_invalid_log_format_rejected():
    with pytest.raises(ValueError, match="LOG_FORMAT"):
        Settings.from_env({"ICEBERGFLOW_LOG_FORMAT": "xml"})
