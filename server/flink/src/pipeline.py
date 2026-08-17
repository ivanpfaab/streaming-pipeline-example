"""Streaming stub: Kafka CSV in, enriched CSV under data/output."""
from datetime import datetime, timezone
from pathlib import Path

from pyflink.common import SimpleStringSchema, Types, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.functions import MapFunction

from config import Config
from utils.common.csv_format import append_csv_message, decode_record, encode_record


def enrich(raw: str) -> str:
    fieldnames, row = decode_record(raw)
    return encode_record(
        row,
        fieldnames,
        extra={
            "processed_at": datetime.now(timezone.utc).isoformat(),
            "processor": "flink",
        },
    )


class WriteCsv(MapFunction):
    def __init__(self, output_dir: str) -> None:
        self.output_dir = output_dir
        self._file = None
        self._header_written = False

    def open(self, runtime_context) -> None:
        path = Path(self.output_dir) / f"flink-{runtime_context.get_index_of_this_subtask()}.csv"
        path.parent.mkdir(parents=True, exist_ok=True)
        self._file = path.open("a", encoding="utf-8", newline="")
        self._header_written = path.stat().st_size > 0

    def map(self, value: str) -> str:
        self._header_written = append_csv_message(self._file, value, self._header_written)
        return value

    def close(self) -> None:
        if self._file is not None:
            self._file.close()


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
            .map(enrich, output_type=Types.STRING())
            .map(WriteCsv(output_dir), output_type=Types.STRING())
        )
        stream.print()

        print(
            f"Flink reading {self.config.kafka_topic} "
            f"from {self.config.kafka_bootstrap} -> {output_dir}/flink-*.csv",
            flush=True,
        )
        env.execute("csv-stream-flink")
