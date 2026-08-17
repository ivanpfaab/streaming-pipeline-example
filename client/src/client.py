"""Rate-limited CSV producer that publishes rows to Kafka."""

from __future__ import annotations

import csv
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from config import Config


class Client:
    def __init__(self, config: Config) -> None:
        self.config = config
        self._producer: KafkaProducer | None = None

    def __call__(self) -> None:
        self.run()

    def run(self) -> None:
        fieldnames, rows = self._load_rows(self.config.input_path)
        self._producer = self._connect_producer(self.config.kafka_bootstrap)
        interval = 1.0 / self.config.rows_per_second
        next_send = time.monotonic()
        sent = 0

        print(
            f"Producing {len(rows)} rows from {self.config.input_path} "
            f"to {self.config.kafka_topic} "
            f"at {self.config.rows_per_second:g} rows/s",
            flush=True,
        )

        try:
            while True:
                for row in rows:
                    payload = {
                        "client_id": self.config.client_id,
                        "ts": datetime.now(timezone.utc).isoformat(),
                        "row": row,
                    }
                    self._producer.send(self.config.kafka_topic, value=payload)
                    sent += 1
                    next_send += interval
                    delay = next_send - time.monotonic()
                    if delay > 0:
                        time.sleep(delay)
        except KeyboardInterrupt:
            print("Stopping producer", flush=True)
        finally:
            self._producer.flush()
            self._producer.close()
            print(f"Sent {sent} records from columns {fieldnames}", flush=True)

    def _load_rows(self, path: Path) -> tuple[list[str], list[dict[str, str]]]:
        if not path.is_file():
            print(f"Input CSV not found: {path}", file=sys.stderr)
            sys.exit(1)

        with path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames is None:
                print(f"Input CSV has no header: {path}", file=sys.stderr)
                sys.exit(1)
            rows = list(reader)

        if not rows:
            print(f"Input CSV has no data rows: {path}", file=sys.stderr)
            sys.exit(1)
        return reader.fieldnames, rows

    def _connect_producer(self, bootstrap: str) -> KafkaProducer:
        last_error: Exception | None = None
        for attempt in range(1, 31):
            try:
                return KafkaProducer(
                    bootstrap_servers=bootstrap.split(","),
                    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                    linger_ms=0,
                    acks="all",
                )
            except NoBrokersAvailable as exc:
                last_error = exc
                print(
                    f"Kafka not ready at {bootstrap} (attempt {attempt}/30), retrying...",
                    file=sys.stderr,
                )
                time.sleep(2)
        print(f"Could not connect to Kafka at {bootstrap}: {last_error}", file=sys.stderr)
        sys.exit(1)
