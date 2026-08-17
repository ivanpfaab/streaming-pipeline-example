# Streaming Pipeline Example

CSV clients produce rows at a configurable rate. A server consumes the same Kafka topic with **Apache Flink** or **Apache Spark**. Kafka is ingest only.

```
data/input CSVs → client-1..N → Kafka topic `events` (key = conversion|page_view)
                              → PyFlink or PySpark → files under data/output
```

The Kafka key comes from the CSV `is_conversion` column (`conversion` vs `page_view`). The topic has **2 partitions** so Flink `keyBy` / Spark `groupBy` stay aligned with the broker.

## Layout

```
client/              CSV producer (Docker image)
server/flink/        PyFlink job (Docker image)
server/spark/        PySpark job (Docker image)
utils/common/        Shared CSV + affinity helpers
data/input/          CSVs the clients read
data/output/         Files the server writes (local bind-mount)
scripts/             generate-compose
k8s/                 Kubernetes manifests (kubectl / kustomize)
terraform/           EKS, AKS, GKE + shared workload module
```

Images are built from the **repo root** so Dockerfiles can `COPY utils`.

## Local (two terminals)

```bash
make generate-compose
```

Prompts: client count, rows/sec, per-client CSV under `data/input/`, Flink vs Spark. Writes gitignored `docker-compose.generated.yml`.

**Terminal 1**

```bash
make server
```

**Terminal 2**

```bash
make client
```

```bash
make client-logs CLIENT_ID=client-1
make server-logs
make down
```

`make server` / `make client` fail if the generated Compose file is missing.

## Cloud

Same images as local. Terraform creates a small Kubernetes cluster, a container registry, then applies the workload (Kafka, topic init, one processor, one Deployment per client). **Apply Flink or Spark, not both** (`pipeline = "flink"` or `"spark"`).

Image build and push are **manual** (no CI in this repo). Clusters cost money; `terraform destroy` when you are done.

### 1. Build images (repo root)

```bash
docker build -f client/Dockerfile -t streaming-client:latest .
docker build -f server/flink/Dockerfile -t streaming-flink:latest .
docker build -f server/spark/Dockerfile -t streaming-spark:latest .
```

Spark is only required if `pipeline = "spark"`.

### 2. Create the cluster and registry first

The Kubernetes provider needs a live cluster. Apply infrastructure before the workload (and before pushing images, so registry URLs exist).

**AWS (EKS + ECR)** — from `terraform/aws`, with AWS CLI credentials:

```bash
cp terraform.tfvars.example terraform.tfvars
terraform init
terraform apply \
  -target=module.vpc \
  -target=module.eks \
  -target=aws_ecr_repository.client \
  -target=aws_ecr_repository.flink \
  -target=aws_ecr_repository.spark
```

**Azure (AKS + ACR)** — from `terraform/azure`, with `az login` and `subscription_id` in `terraform.tfvars`:

```bash
cp terraform.tfvars.example terraform.tfvars
terraform init
terraform apply \
  -target=azurerm_resource_group.this \
  -target=azurerm_container_registry.this \
  -target=azurerm_kubernetes_cluster.this \
  -target=azurerm_role_assignment.acr
```

**GCP (GKE + Artifact Registry)** — from `terraform/gcp`, with `gcloud auth application-default login` and `project_id` in `terraform.tfvars`:

```bash
cp terraform.tfvars.example terraform.tfvars
terraform init
terraform apply \
  -target=google_project_service.container \
  -target=google_project_service.artifactregistry \
  -target=google_artifact_registry_repository.this \
  -target=google_artifact_registry_repository_iam_member.nodes \
  -target=google_container_cluster.this
```

### 3. Push images

Use the image URLs from `terraform output`. Tag and push the client image plus the processor you selected.

**ECR**

```bash
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin "$(terraform output -raw client_image | cut -d/ -f1)"
docker tag streaming-client:latest "$(terraform output -raw client_image)"
docker tag streaming-flink:latest "$(terraform output -raw flink_image)"
docker push "$(terraform output -raw client_image)"
docker push "$(terraform output -raw flink_image)"
```

**ACR**

```bash
az acr login --name "$(terraform output -raw acr_login_server | cut -d. -f1)"
docker tag streaming-client:latest "$(terraform output -raw client_image)"
docker tag streaming-flink:latest "$(terraform output -raw processor_image)"
docker push "$(terraform output -raw client_image)"
docker push "$(terraform output -raw processor_image)"
```

**Artifact Registry**

```bash
gcloud auth configure-docker us-central1-docker.pkg.dev
docker tag streaming-client:latest "$(terraform output -raw client_image)"
docker tag streaming-flink:latest "$(terraform output -raw processor_image)"
docker push "$(terraform output -raw client_image)"
docker push "$(terraform output -raw processor_image)"
```

For Spark, tag/push `streaming-spark:latest` to `spark_image` (AWS) or `processor_image` (Azure/GCP) instead of Flink.

### 4. Deploy the workload

```bash
terraform apply
```

Point kubectl at the cluster with `terraform output -raw kubeconfig_command`, then:

```bash
kubectl -n streaming get pods
kubectl -n streaming logs -f deploy/processor
kubectl -n streaming logs -f deploy/client-1
kubectl -n streaming exec deploy/processor -- ls /data/output
```

If pods started before the push finished:

```bash
kubectl -n streaming rollout restart deploy
```

### Optional: kubectl / kustomize without Terraform workload

After you have a cluster and have pushed images, you can apply [`k8s/`](k8s/) yourself. Default kustomization uses Flink:

```bash
cd k8s
kustomize edit set image IMAGE_PROCESSOR=REGISTRY/streaming-flink:latest IMAGE_CLIENT=REGISTRY/streaming-client:latest
kubectl apply -k .
```

For Spark, change `kustomization.yaml` to `processor-spark.yaml` and `streaming-spark`.

### Tear down

```bash
terraform destroy
```

## Variables

| Variable | Meaning |
| --- | --- |
| `pipeline` | `flink` or `spark` |
| `rows_per_second` | Rate applied to every client |
| `image_tag` | Tag pushed to the registry (`latest` by default) |
| `clients` | List of `{ id, csv }` — CSV names must exist under `data/input/` |
