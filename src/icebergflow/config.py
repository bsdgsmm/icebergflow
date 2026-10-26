"""Runtime settings resolved from environment variables."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

ENV_PREFIX = "ICEBERGFLOW_"


@dataclass(frozen=True)
class Settings:
    raw_dir: Path = Path("data/raw")
    warehouse_dir: Path = Path("warehouse")
    jars_dir: Path = Path("jars")
    catalog: str = "lakehouse"
    spark_master: str = "local[*]"
    driver_memory: str = "2g"
    shuffle_partitions: int = 4
    log_level: str = "INFO"
    log_format: str = "text"

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> Settings:
        env = os.environ if environ is None else environ

        def get(name: str, default: str) -> str:
            value = env.get(f"{ENV_PREFIX}{name}", "").strip()
            return value or default

        defaults = cls()
        shuffle = get("SHUFFLE_PARTITIONS", str(defaults.shuffle_partitions))
        if not shuffle.isdigit() or int(shuffle) < 1:
            raise ValueError(f"{ENV_PREFIX}SHUFFLE_PARTITIONS must be a positive integer")

        log_format = get("LOG_FORMAT", defaults.log_format).lower()
        if log_format not in {"text", "json"}:
            raise ValueError(f"{ENV_PREFIX}LOG_FORMAT must be 'text' or 'json'")

        return cls(
            raw_dir=Path(get("RAW_DIR", str(defaults.raw_dir))),
            warehouse_dir=Path(get("WAREHOUSE_DIR", str(defaults.warehouse_dir))),
            jars_dir=Path(get("JARS_DIR", str(defaults.jars_dir))),
            catalog=get("CATALOG", defaults.catalog),
            spark_master=get("SPARK_MASTER", defaults.spark_master),
            driver_memory=get("DRIVER_MEMORY", defaults.driver_memory),
            shuffle_partitions=int(shuffle),
            log_level=get("LOG_LEVEL", defaults.log_level).upper(),
            log_format=log_format,
        )
