"""Rate-limited CSV producer that publishes rows to Kafka."""

from __future__ import annotations

import sys
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from config import Config
from utils.common.affinity import affinity_key
from utils.common.csv_format import encode_record, read_rows


class Client:
    def __init__(self, config: Config) -> None:
        self.config = config
        self._producer: KafkaProducer | None = None

    def __call__(self) -> None:
        self.run()

    def run(self) -> None:
        try:
            fieldnames, rows = read_rows(self.config.input_path)
        except (FileNotFoundError, ValueError) as exc:
            print(exc, file=sys.stderr)
            sys.exit(1)

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
                    ts = datetime.now(timezone.utc).isoformat()
                    key = affinity_key(row)
                    payload = encode_record(
                        row,
                        fieldnames,
                        extra={
                            "client_id": self.config.client_id,
                            "ts": ts,
                        },
                    )
                    self._producer.send(
                        self.config.kafka_topic,
                        key=key,
                        value=payload,
                    )
                    sent += 1
                    print(
                        f"streamed #{sent} key={key} {payload.splitlines()[-1]}",
                        flush=True,
                    )
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

    def _connect_producer(self, bootstrap: str) -> KafkaProducer:
        last_error: Exception | None = None
        for attempt in range(1, 31):
            try:
                return KafkaProducer(
                    bootstrap_servers=bootstrap.split(","),
                    key_serializer=lambda key: key.encode("utf-8"),
                    value_serializer=lambda value: value.encode("utf-8"),
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
