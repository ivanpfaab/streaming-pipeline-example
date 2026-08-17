terraform {
  required_version = ">= 1.5.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.14"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.33"
    }
  }
}

variable "project_id" {
  type = string
}

variable "region" {
  type    = string
  default = "us-central1"
}

variable "zone" {
  type    = string
  default = "us-central1-a"
}

variable "cluster_name" {
  type    = string
  default = "streaming-example"
}

variable "pipeline" {
  type    = string
  default = "flink"
}

variable "image_tag" {
  type    = string
  default = "latest"
}

variable "rows_per_second" {
  type    = number
  default = 5
}

variable "clients" {
  type = list(object({
    id  = string
    csv = string
  }))
  default = [
    { id = "client-1", csv = "sample.csv" },
    { id = "client-2", csv = "events.csv" },
  ]
}

provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_project_service" "container" {
  service            = "container.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "artifactregistry" {
  service            = "artifactregistry.googleapis.com"
  disable_on_destroy = false
}

resource "google_artifact_registry_repository" "this" {
  location      = var.region
  repository_id = var.cluster_name
  format        = "DOCKER"

  depends_on = [google_project_service.artifactregistry]
}

resource "google_container_cluster" "this" {
  name                = var.cluster_name
  location            = var.zone
  deletion_protection = false
  initial_node_count  = 2

  node_config {
    machine_type = "e2-standard-4"
    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform",
    ]
  }

  depends_on = [google_project_service.container]
}

data "google_client_config" "default" {}

data "google_project" "this" {
  project_id = var.project_id
}

resource "google_artifact_registry_repository_iam_member" "nodes" {
  location   = google_artifact_registry_repository.this.location
  repository = google_artifact_registry_repository.this.name
  role       = "roles/artifactregistry.reader"
  member     = "serviceAccount:${data.google_project.this.number}-compute@developer.gserviceaccount.com"
}

provider "kubernetes" {
  host                   = "https://${google_container_cluster.this.endpoint}"
  token                  = data.google_client_config.default.access_token
  cluster_ca_certificate = base64decode(google_container_cluster.this.master_auth[0].cluster_ca_certificate)
}

locals {
  registry       = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.this.repository_id}"
  processor_name = var.pipeline == "spark" ? "spark" : "flink"
}

module "workload" {
  source = "../modules/workload"

  pipeline        = var.pipeline
  client_image    = "${local.registry}/streaming-client:${var.image_tag}"
  processor_image = "${local.registry}/streaming-${local.processor_name}:${var.image_tag}"
  rows_per_second = var.rows_per_second
  clients         = var.clients

  depends_on = [
    google_container_cluster.this,
    google_artifact_registry_repository_iam_member.nodes,
  ]
}

output "registry" {
  value = local.registry
}

output "client_image" {
  value = "${local.registry}/streaming-client:${var.image_tag}"
}

output "processor_image" {
  value = "${local.registry}/streaming-${local.processor_name}:${var.image_tag}"
}

output "kubeconfig_command" {
  value = "gcloud container clusters get-credentials ${google_container_cluster.this.name} --zone ${var.zone} --project ${var.project_id}"
}
