"""Flink job configuration loaded from environment variables."""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path


def _require(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None or value.strip() == "":
        print(f"Missing required environment variable: {name}", file=sys.stderr)
        sys.exit(1)
    return value


@dataclass(frozen=True)
class Config:
    kafka_bootstrap: str
    kafka_topic: str
    output_dir: Path
    kafka_connector_jar: Path

    @classmethod
    def from_env(cls) -> Config:
        return cls(
            kafka_bootstrap=_require("KAFKA_BOOTSTRAP", "kafka:9092"),
            kafka_topic=_require("KAFKA_TOPIC", "events"),
            output_dir=Path(_require("OUTPUT_DIR", "/data/output")),
            kafka_connector_jar=Path(
                _require(
                    "KAFKA_CONNECTOR_JAR",
                    "/opt/flink/lib/flink-sql-connector-kafka-3.4.0-1.20.jar",
                )
            ),
        )


if __name__ == "__main__":
    from pipeline import Pipeline

    Pipeline(Config.from_env())()
