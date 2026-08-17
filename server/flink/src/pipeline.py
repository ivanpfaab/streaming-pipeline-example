"""Count events by affinity key (conversion vs page_view) on one Kafka topic."""

from datetime import datetime, timezone
from pathlib import Path

from pyflink.common import SimpleStringSchema, Types, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import MapFunction, RichMapFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource

from config import Config
from utils.common.affinity import affinity_key
from utils.common.csv_format import append_csv_message, decode_record, encode_record

COUNT_FIELDS = ["affinity_key", "event_count"]


def _count_record(key: str, count: int, processor: str) -> str:
    return encode_record(
        {"affinity_key": key, "event_count": str(count)},
        COUNT_FIELDS,
        extra={
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "processor": processor,
        },
    )


class RunningCount(RichMapFunction):
    """Keyed running total. Swap this map for a windowed aggregate later."""

    def open(self, runtime_context) -> None:
        self._count = runtime_context.get_state(
            ValueStateDescriptor("event_count", Types.LONG())
        )

    def map(self, raw: str) -> str:
        _, row = decode_record(raw)
        key = affinity_key(row)
        current = self._count.value()
        current = 0 if current is None else current
        current += 1
        self._count.update(current)
        return _count_record(key, current, "flink")


class WriteCounts(MapFunction):
    def __init__(self, output_dir: str) -> None:
        self.output_dir = output_dir
        self._files = None
        self._headers = None

    def open(self, _runtime_context) -> None:
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        self._files = {}
        self._headers = {}

    def map(self, value: str) -> str:
        _, row = decode_record(value)
        key = row.get("affinity_key", "unknown")
        if key not in self._files:
            path = Path(self.output_dir) / f"flink-{key}.csv"
            already = path.exists() and path.stat().st_size > 0
            self._files[key] = path.open("a", encoding="utf-8", newline="")
            self._headers[key] = already
        self._headers[key] = append_csv_message(
            self._files[key], value, self._headers[key]
        )
        return value

    def close(self) -> None:
        for handle in (self._files or {}).values():
            handle.close()


class Pipeline:
    def __init__(self, config: Config) -> None:
        self.config = config

    def __call__(self) -> None:
        self.run()

    def run(self) -> None:
        env = StreamExecutionEnvironment.get_execution_environment()
        env.add_jars(f"file://{self.config.kafka_connector_jar}")

        source = (
            KafkaSource.builder()
            .set_bootstrap_servers(self.config.kafka_bootstrap)
            .set_topics(self.config.kafka_topic)
            .set_group_id("flink-pipeline")
            .set_starting_offsets(KafkaOffsetsInitializer.earliest())
            .set_value_only_deserializer(SimpleStringSchema())
            .build()
        )

        output_dir = str(self.config.output_dir)
        stream = (
            env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka")
            .key_by(
                lambda raw: affinity_key(decode_record(raw)[1]),
                key_type=Types.STRING(),
            )
            .map(RunningCount(), output_type=Types.STRING())
            .map(WriteCounts(output_dir), output_type=Types.STRING())
        )
        stream.print()

        print(
            f"Flink counting {self.config.kafka_topic} by affinity "
            f"from {self.config.kafka_bootstrap} -> {output_dir}/flink-*.csv",
            flush=True,
        )
        env.execute("csv-stream-flink")
