# Apache Flink vs Apache Beam

This repo processes the same Kafka stream with **PyFlink** (and PySpark). Beam is the other name that usually comes up in the same conversation. They are easy to mix up: both do event-time streaming, both have Python APIs, and you can even run Beam *on* Flink. They are not the same kind of thing.

**Flink is an engine. Beam is a portable programming model.** That distinction explains most of the rest.

## A short history

**Flink** grew out of Stratosphere, a research system at TU Berlin (around 2010) aimed at cyclic dataflows and in-memory processing beyond Hadoop MapReduce. It entered the Apache Incubator in 2014 and became a top-level project in 2015. The bet was **stream-first**: a continuous dataflow of records, with batch as a finite stream, rather than Spark’s original micro-batch model. Production features piled up around that runtime — keyed state, checkpointing, savepoints, watermarks, event-time windows, backpressure. The DataStream API (Java, then Scala, then Python as PyFlink) talks to that engine directly.

**Beam** is younger as an Apache project (2016) but older as an idea. Google had FlumeJava for batch and MillWheel for streaming, then published *The Dataflow Model* (2015) as a unified way to talk about **event time, watermarks, windows, and triggers** for both bounded and unbounded data. Google Cloud Dataflow implemented that model as a managed service. The SDK was donated to Apache as Beam so the same pipeline could run on more than one backend.

So: Flink is “we built a streaming system.” Beam is “we named the model and made the SDK runner-agnostic.” A lot of the vocabulary people treat as “Beam concepts” (watermarks, allowed lateness, triggers) also exists in Flink, because both are implementations of the same generation of streaming theory.

## What you actually run

```
Beam pipeline  →  runner (Dataflow, Flink, Spark, Direct, …)  →  cluster / service
Flink job      →  Flink runtime (JobManager + TaskManagers)
```

With Flink you submit a job to Flink. The API, the scheduler, state backends, checkpoints, and the web UI are all Flink.

With Beam you write `Pipeline | Read | ParDo | WindowInto | GroupByKey | Write` against **PCollections**. That graph is translated for a **runner**:

- **DirectRunner** — local, for tests
- **DataflowRunner** — Google’s managed service
- **FlinkRunner** — Beam job executed as a Flink job
- **SparkRunner** — same idea on Spark

Choosing Beam does not remove the need to choose (and operate) a runtime. If the runner is Flink, you still have a Flink cluster; you have just chosen not to use Flink’s own API.

This demo uses Flink’s API, not Beam-on-Flink. The goal here is “same topology, two engines” (Flink vs Spark), which is the opposite of Beam’s goal (“one pipeline, many engines”).

## How the APIs feel

**Flink DataStream** (what `server/flink` uses) is an execution graph: source → `key_by` → keyed `map` with `ValueState` → sink. You are close to the runtime. Keyed state lives on the key group; checkpoints snapshot it. Windows, timers, and `ProcessFunction` are there when you need them. Flink SQL / Table API sit one layer up if you would rather declare the job than wire operators.

**Beam** is transforms on collections. You rarely mention keys as a shuffle primitive in the same way; `GroupByKey` and combining are explicit steps, windowing is a transform (`WindowInto`), and side inputs / stateful `DoFn`s cover joins and running aggregates. The code looks similar in Java, Python, and Go because the model is the product.

Practically:

| | Flink | Beam |
| --- | --- | --- |
| Unit of work | Stream of records, operators | `PCollection` + `PTransform` |
| Keys | `keyBy` / keyed state | `GroupByKey`, per-key state in `DoFn` |
| Windows | Assigned on the stream | `WindowInto` as a transform |
| SQL | Flink SQL is a first-class path | Beam SQL exists; less central |
| Python | PyFlink, still a JVM job (`pemja` / gateway) | Beam Python SDK; often a portable SDK harness next to the runner |

Neither Python story is “pure CPython streaming.” Flink’s Python job in this repo still needs a JDK. Beam Python typically ships user code in an SDK worker and talks to the runner over a portability protocol. Both are fine; both surprise people who expected a single `python` process.

## Semantics: closer than the marketing

For a Kafka topic with event-time windows, the *ideas* match:

- **Event time vs processing time** — when the event happened vs when the engine saw it
- **Watermarks** — “I believe event time has reached T; close windows that end before T”
- **Late data** — records behind the watermark; drop, allowed lateness, or side output
- **Triggers** (Beam’s word) / **window firing** (Flink) — when a window emits: on watermark, early speculative results, late refinements

Beam’s API makes that policy very explicit (`AfterWatermark`, `AfterCount`, accumulating vs discarding fired panes). Flink expresses the same things with window assigners, triggers, and process functions, but many jobs never leave the defaults.

**State and exactly-once** are where “engine vs model” shows up. Flink’s checkpoint barrier algorithm, keyed state, and two-phase-commit sinks are native. Beam *has* state and timers in the model; quality depends on the runner. Dataflow and the Flink runner are the usual choices when you care. A Beam pipeline on a weak runner can look like the same code and not give you the same guarantees.

**Batch vs streaming:** Beam treated bounded and unbounded `PCollection`s as one API from day one. Flink converged on the same view (bounded sources are finite streams; batch and stream share operators). Spark Structured Streaming is a third design (unbounded incremental DataFrames). This repo’s Spark path is that third design, not Beam.

## Operations

Flink ops are Flink ops: job submit, checkpoints, savepoints (versioned, resumable job state), parallelism, backpressure in the UI, TaskManager memory, Kafka consumer group + offsets aligned with checkpoints.

Beam ops are **runner ops**:

- On **Dataflow**, Google autoscales workers, owns the service, and bills by job. You debug in the Dataflow UI.
- On **FlinkRunner**, you still size TaskManagers, tune checkpoints, and read Flink’s UI — plus Beam-specific packaging (SDK harness, job server, portable pipeline artifacts).

Portability has a cost: a Beam-on-Flink job is usually harder to reason about than the equivalent DataStream job, because there are two layers of graph (Beam → Flink). Portability has a payoff: the same Python pipeline can be tested with DirectRunner and run on Dataflow without a rewrite.

## When to use which

**Use Flink’s API** when the runtime is Flink (or will be), you want keyed state and savepoints as first-class tools, and you are willing to write Flink. This repo is that case.

**Use Beam** when you want one pipeline definition across clouds or engines (especially Dataflow + something else), or a team standardized on Beam’s Java/Python SDK and does not want a Flink-shaped codebase. Then you pick the runner as a deployment detail.

**Use both** only in the “Beam pipeline, Flink runner” sense. That is a valid production pattern. It is not a way to get Flink and Spark “for free” from one job without accepting runner limitations (the Spark runner is not equivalent to Flink for streaming state).

## Further reading

- Tyler Akidau et al., [*The Dataflow Model*](https://research.google/pubs/pub43864/) (VLDB 2015) — the paper Beam is built on
- [Apache Flink documentation](https://flink.apache.org/) — DataStream, state, checkpoints
- [Apache Beam documentation](https://beam.apache.org/) — model, runners, Python SDK
- Flink’s own take: [Apache Beam Capability Matrix](https://beam.apache.org/documentation/runners/capability-matrix/) — what each runner actually implements
