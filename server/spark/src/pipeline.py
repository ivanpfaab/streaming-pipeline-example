"""Streaming stub: Kafka CSV in, enriched CSV under data/output."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from pyspark import TaskContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

from config import Config
from utils.common.csv_format import append_csv_message, decode_record, encode_record


def enrich(raw: str) -> str:
    fieldnames, row = decode_record(raw)
    return encode_record(
        row,
        fieldnames,
        extra={
            "processed_at": datetime.now(timezone.utc).isoformat(),
            "processor": "spark",
        },
    )


enrich_udf = udf(enrich, StringType())


class Pipeline:
    def __init__(self, config: Config) -> None:
        self.config = config

    def __call__(self) -> None:
        self.run()

    def run(self) -> None:
        spark = (
            SparkSession.builder.appName("csv-stream-spark")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")

        stream = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.config.kafka_bootstrap)
            .option("subscribe", self.config.kafka_topic)
            .option("startingOffsets", "earliest")
            .load()
            .selectExpr("CAST(value AS STRING) AS value")
            .withColumn("value", enrich_udf(col("value")))
        )

        output_dir = str(self.config.output_dir)
        checkpoint = str(self.config.output_dir / ".spark-checkpoint")
        print(
            f"Spark reading {self.config.kafka_topic} "
            f"from {self.config.kafka_bootstrap} -> {output_dir}/spark-*.csv",
            flush=True,
        )

        (
            stream.writeStream.foreachBatch(self._write_batch(output_dir))
            .option("checkpointLocation", checkpoint)
            .start()
            .awaitTermination()
        )

    def _write_batch(self, output_dir: str):
        def write_batch(batch_df, _batch_id: int) -> None:
            batch_df.show(truncate=False)
            batch_df.foreachPartition(lambda rows: _write_partition(output_dir, rows))

        return write_batch


def _write_partition(output_dir: str, rows) -> None:
    context = TaskContext.get()
    partition = context.partitionId() if context is not None else 0
    path = Path(output_dir) / f"spark-{partition}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    header_written = path.exists() and path.stat().st_size > 0
    with path.open("a", encoding="utf-8", newline="") as handle:
        for record in rows:
            header_written = append_csv_message(handle, record.value, header_written)
