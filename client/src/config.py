"""Client configuration loaded from environment variables."""

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
    input_path: Path
    rows_per_second: float
    kafka_bootstrap: str
    kafka_topic: str
    client_id: str

    @classmethod
    def from_env(cls) -> Config:
        rows_per_second = float(_require("ROWS_PER_SECOND"))
        if rows_per_second <= 0:
            print("ROWS_PER_SECOND must be greater than 0", file=sys.stderr)
            sys.exit(1)

        return cls(
            input_path=Path(_require("INPUT_PATH")),
            rows_per_second=rows_per_second,
            kafka_bootstrap=_require("KAFKA_BOOTSTRAP", "kafka:9092"),
            kafka_topic=_require("KAFKA_TOPIC", "events"),
            client_id=_require("CLIENT_ID", "client"),
        )


if __name__ == "__main__":
    from client import Client

    Client(Config.from_env())()
