"""Count events by affinity key (conversion vs page_view) on one Kafka topic."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

from config import Config
from utils.common.affinity import affinity_from_csv_value
from utils.common.csv_format import append_csv_message, encode_record

COUNT_FIELDS = ["affinity_key", "event_count"]

affinity_udf = udf(affinity_from_csv_value, StringType())


def _count_record(key: str, count: int) -> str:
    return encode_record(
        {"affinity_key": key, "event_count": str(count)},
        COUNT_FIELDS,
        extra={
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "processor": "spark",
        },
    )


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

        # Running keyed count. Later: groupBy(window(...), "affinity_key").
        stream = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.config.kafka_bootstrap)
            .option("subscribe", self.config.kafka_topic)
            .option("startingOffsets", "earliest")
            .load()
            .selectExpr("CAST(value AS STRING) AS value")
            .withColumn("affinity_key", affinity_udf(col("value")))
            .groupBy("affinity_key")
            .count()
        )

        output_dir = str(self.config.output_dir)
        checkpoint = str(self.config.output_dir / ".spark-checkpoint")
        print(
            f"Spark counting {self.config.kafka_topic} by affinity "
            f"from {self.config.kafka_bootstrap} -> {output_dir}/spark-*.csv",
            flush=True,
        )

        (
            stream.writeStream.outputMode("update")
            .foreachBatch(self._write_batch(output_dir))
            .option("checkpointLocation", checkpoint)
            .start()
            .awaitTermination()
        )

    def _write_batch(self, output_dir: str):
        def write_batch(batch_df, _batch_id: int) -> None:
            batch_df.show(truncate=False)
            Path(output_dir).mkdir(parents=True, exist_ok=True)
            for record in batch_df.collect():
                key = record["affinity_key"]
                payload = _count_record(key, int(record["count"]))
                path = Path(output_dir) / f"spark-{key}.csv"
                already = path.exists() and path.stat().st_size > 0
                with path.open("a", encoding="utf-8", newline="") as handle:
                    append_csv_message(handle, payload, already)

        return write_batch
