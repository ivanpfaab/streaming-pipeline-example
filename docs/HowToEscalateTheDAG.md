# How to escalate the DAG

The jobs in this repo are a **short chain**, not a rich graph. That is deliberate: one Kafka topic, one affinity key, a running count, a file sink. Escalating the DAG means growing that chain into a real streaming topology **without throwing away the keying**. Windows, branches, and joins all sit on the same `events` stream.

If you add a topic per event type, you have split the pipeline at the broker instead of in the job. Do not do that. The Kafka key (`conversion` | `page_view`) plus Flink `keyBy` / Spark `groupBy` already collocate work. New operators should reuse that key.

## What you have today

```
CSV → client → Kafka topic `events` (2 partitions, key = affinity)
            → source → keyBy / groupBy(affinity) → running count → CSV sink
```

Flink (`server/flink/src/pipeline.py`): `KafkaSource` → `key_by(affinity)` → `RunningCount` (keyed `ValueState`) → `WriteCounts`. Watermarks are off (`WatermarkStrategy.no_watermarks()`).

Spark (`server/spark/src/pipeline.py`): `readStream` Kafka → UDF for affinity → `groupBy("affinity_key").count()` → `foreachBatch` files. Comment in place: later `groupBy(window(...), "affinity_key")`.

That graph is a **line**. Every record follows the same operators. A DAG starts when the job **forks, joins, or changes the time domain** — still one source topic.

## What “escalate” is not

Raising TaskManager count, `parallelism`, or Kubernetes replicas **scales the same DAG**. It does not add edges. Flink will already run the two affinity keys on different key groups when parallelism ≥ 2; Spark will shuffle on `affinity_key`. Ops changes belong in deploy, not in the job graph.

Escalating the DAG is adding **operators and connections**: windows, filters that split the stream, unions, joins, extra sinks.

## Ladder

Stay on one topic. Each step below can land in both Flink and Spark with the same meaning, different API. Beam would express the same ladder as `WindowInto` / `ParDo` / `GroupByKey` if you ever moved the job there ([Beam vs Flink](BeamVsFlink.md)).

### 1. Window the keyed stream

The count is already per key for the life of the job. The next honest product question is “how many conversions in the last 10 seconds?” not “how many since the process started?”

Keep `keyBy` / `groupBy(affinity_key)`. Replace the unbounded aggregate with a **tumbling** (or sliding / session) window.

- Flink: `key_by(...)` then `.window(TumblingEventTimeWindows.of(...)).aggregate(...)` instead of `RunningCount`. The comment on that class is the hook.
- Spark: `.groupBy(window(col("event_time"), "10 seconds"), "affinity_key").count()`.

Keyed state does not go away; it becomes **per key per window**. No new Kafka partitions.

### 2. Switch on event time

Windows on **processing time** close when the clock on the worker says so. That is easy and wrong for anything you might replay from Kafka.

The client already stamps `ts` on every record (ISO UTC). Promote that to the event timestamp:

- Flink: parse `ts`, `WatermarkStrategy.for_bounded_out_of_orderness(...)` with a timestamp assigner. Drop `no_watermarks()`.
- Spark: parse `ts` to a timestamp column and set `withWatermark("event_time", "...")` before the windowed `groupBy`.

**Watermark** = “I believe event time has reached T; windows that ended before T may close.” Late records (behind the watermark) need a policy: drop, allowed lateness, or a side output. You do not need a custom trigger to start; defaults plus a bounded out-of-orderness are enough for this demo.

Until watermarks exist, treat windowed counts as a local experiment, not a replayable metric.

### 3. Branch the graph

A DAG appears when **one stream feeds two paths**.

Examples that fit this data:

- **Split:** `is_conversion` already encoded in the key. You can still `filter` into a conversions-only path (alerts, a different sink) while the main path keeps windowed counts for both keys.
- **Two sinks:** same windowed table → file **and** stdout / Kafka. Flink: two `.add_sink` / `sink_to` from the same DataStream (or `Union` of two keyed streams). Spark: two `writeStream` queries on the same derived DataFrame, each with its own checkpoint.
- **Side output (Flink):** late events or poison rows go to a tagged stream instead of poisoning the count.

```
events → keyBy(affinity) → windowed count → counts sink
                       ↘ filter(conversion) → alert sink
```

That is still one consumer group, one topic. The fork is in the job.

### 4. Join or enrich

Only after the keyed, windowed path is stable. Typical next edges:

- **Stream–stream join** on a time window (e.g. page_view then conversion for the same `id` within 5 minutes). That needs a **join key** (`id`), which is *not* the affinity key. You will `keyBy(id)` for the join and keep affinity for the count path — two different shuffles, still one topic.
- **Stream–static enrich** (dimension lookup): broadcast / side input of a small table (campaign, geo). Does not require new Kafka.

Joins are where the graph stops being obvious in the Flink UI / Spark DAG. Name operators. Keep the count pipeline as its own branch so a join failure does not block the metric.

### 5. Timers and process functions (when SQL is not enough)

Flink `ProcessFunction` + event-time timers: “emit if no conversion arrived within 15 minutes of this page view.” That is a DAG node with **state + timer**, not another window aggregate. Spark Structured Streaming does not have the same timer primitive; you approximate with watermarks and stream-stream join, or you leave that branch on Flink.

If the job is mostly aggregations, **Flink SQL / Spark SQL** can replace a lot of DataStream/DataFrame wiring. Escalate to SQL when the DAG is relational; stay in process functions when it is “wait, then emit.”

## Order of operations

1. Keep one topic and the affinity key.
2. Add event-time timestamps + watermarks.
3. Window the existing keyed count.
4. Then fork sinks or filters.
5. Then joins / timers.

Skipping to joins while still on processing-time running totals makes the graph look grown up and the numbers unreproducible.

## Flink vs Spark vs Beam on this ladder

| Step | Flink | Spark | Beam |
| --- | --- | --- | --- |
| Keyed running count | `keyBy` + `ValueState` (today) | `groupBy` + `count` (today) | stateful `DoFn` or `Combine` |
| Event time | watermark strategy on the source | `withWatermark` | always in the model; assign timestamps |
| Windows | window assigner on the keyed stream | `groupBy(window, key)` | `WindowInto` then `GroupByKey` |
| Branch | two sinks, `filter`, side outputs | two `writeStream`s | multiple `PTransform` outputs |
| Join | interval join / SQL | stream-stream join | `CoGroupByKey` / join transforms |

Same ladder, different spelling. The Kafka layout does not change.

## What to leave alone

- **Topic-per-affinity** — splits what `keyBy` already does, and makes cross-key windows/joins painful.
- **High-cardinality keys** (raw `id` as the only key for global counts) — state explodes. Affinity is a *coarse* key on purpose; use `id` only on the join branch.
- **File sinks as the source of truth** — `data/output/*.csv` is a demo. A real extra sink is Kafka / a table, with checkpoints (Flink) or `checkpointLocation` (Spark) lined up with the source offsets.
