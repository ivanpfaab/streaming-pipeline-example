# Streaming Pipeline Example

Skeleton for a local streaming demo: CSV clients produce rows at a configurable rate, and a server processes them with **Apache Flink** or **Apache Spark**. Kafka is the ingest bus only.

This repo is built incrementally. The commands below are the intended local flow once the remaining pieces land.

## Layout

```
client/              CSV producer (Docker image)
server/flink/        PyFlink job (Docker image)
server/spark/        PySpark job (Docker image)
data/input/          CSVs the clients read (replace with real datasets later)
data/output/         Files the server writes
scripts/             generate-compose and other helpers
k8s/                 Kubernetes manifests
terraform/aws|azure|gcp
```

## Local flow (two terminals)

1. `make generate-compose` — asks for client count, rows per second, per-client CSV, and Flink vs Spark.
2. Terminal 1: `make server` — Kafka + the chosen processor.
3. Terminal 2: `make client` — one container per client, each reading its assigned CSV.

## Cloud

The same images are meant to deploy to EKS, AKS, or GKE via Terraform. Details in later commits.
