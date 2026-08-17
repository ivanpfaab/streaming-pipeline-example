terraform {
  required_version = ">= 1.5.0"
  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.33"
    }
  }
}

variable "pipeline" {
  type    = string
  default = "flink"
  validation {
    condition     = contains(["flink", "spark"], var.pipeline)
    error_message = "pipeline must be flink or spark."
  }
}

variable "client_image" {
  type = string
}

variable "processor_image" {
  type = string
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

variable "namespace" {
  type    = string
  default = "streaming"
}

resource "kubernetes_namespace_v1" "streaming" {
  metadata {
    name = var.namespace
  }
}

resource "kubernetes_config_map_v1" "input" {
  metadata {
    name      = "input-csvs"
    namespace = kubernetes_namespace_v1.streaming.metadata[0].name
  }
  data = {
    for path in fileset("${path.module}/../../../data/input", "*.csv") :
    path => file("${path.module}/../../../data/input/${path}")
  }
}

resource "kubernetes_service_v1" "kafka" {
  metadata {
    name      = "kafka"
    namespace = kubernetes_namespace_v1.streaming.metadata[0].name
  }
  spec {
    selector = { app = "kafka" }
    port {
      name        = "plaintext"
      port        = 9092
      target_port = 9092
    }
  }
}

resource "kubernetes_deployment_v1" "kafka" {
  metadata {
    name      = "kafka"
    namespace = kubernetes_namespace_v1.streaming.metadata[0].name
  }
  spec {
    replicas = 1
    selector {
      match_labels = { app = "kafka" }
    }
    template {
      metadata {
        labels = { app = "kafka" }
      }
      spec {
        container {
          name  = "kafka"
          image = "apache/kafka:3.9.1"
          port {
            container_port = 9092
          }
          port {
            container_port = 9093
          }
          env {
            name  = "KAFKA_NODE_ID"
            value = "1"
          }
          env {
            name  = "KAFKA_PROCESS_ROLES"
            value = "broker,controller"
          }
          env {
            name  = "KAFKA_LISTENERS"
            value = "PLAINTEXT://:9092,CONTROLLER://:9093"
          }
          env {
            name  = "KAFKA_ADVERTISED_LISTENERS"
            value = "PLAINTEXT://kafka:9092"
          }
          env {
            name  = "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP"
            value = "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT"
          }
          env {
            name  = "KAFKA_CONTROLLER_QUORUM_VOTERS"
            value = "1@kafka:9093"
          }
          env {
            name  = "KAFKA_CONTROLLER_LISTENER_NAMES"
            value = "CONTROLLER"
          }
          env {
            name  = "KAFKA_INTER_BROKER_LISTENER_NAME"
            value = "PLAINTEXT"
          }
          env {
            name  = "CLUSTER_ID"
            value = "4L6g3nShT-eMCtK--X86sw"
          }
          env {
            name  = "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR"
            value = "1"
          }
          env {
            name  = "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR"
            value = "1"
          }
          env {
            name  = "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR"
            value = "1"
          }
          env {
            name  = "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS"
            value = "0"
          }
          env {
            name  = "KAFKA_AUTO_CREATE_TOPICS_ENABLE"
            value = "true"
          }
          readiness_probe {
            tcp_socket {
              port = 9092
            }
            initial_delay_seconds = 15
            period_seconds        = 5
          }
        }
      }
    }
  }
}

resource "kubernetes_job_v1" "kafka_init" {
  metadata {
    name      = "kafka-init"
    namespace = kubernetes_namespace_v1.streaming.metadata[0].name
  }
  spec {
    backoff_limit = 6
    template {
      metadata {}
      spec {
        restart_policy = "OnFailure"
        init_container {
          name    = "wait-kafka"
          image   = "busybox:1.36"
          command = ["sh", "-c", "until nc -z kafka 9092; do sleep 2; done"]
        }
        container {
          name  = "kafka-init"
          image = "apache/kafka:3.9.1"
          command = [
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server", "kafka:9092",
            "--create", "--if-not-exists",
            "--topic", "events",
            "--partitions", "2",
            "--replication-factor", "1",
          ]
        }
      }
    }
  }
  wait_for_completion = true
  timeouts {
    create = "10m"
    update = "10m"
    delete = "5m"
  }
  depends_on = [kubernetes_deployment_v1.kafka, kubernetes_service_v1.kafka]
}

resource "kubernetes_deployment_v1" "processor" {
  metadata {
    name      = "processor"
    namespace = kubernetes_namespace_v1.streaming.metadata[0].name
    labels = {
      app      = "processor"
      pipeline = var.pipeline
    }
  }
  spec {
    replicas = 1
    selector {
      match_labels = { app = "processor" }
    }
    template {
      metadata {
        labels = {
          app      = "processor"
          pipeline = var.pipeline
        }
      }
      spec {
        init_container {
          name    = "wait-kafka"
          image   = "busybox:1.36"
          command = ["sh", "-c", "until nc -z kafka 9092; do sleep 2; done"]
        }
        container {
          name              = "processor"
          image             = var.processor_image
          image_pull_policy = "Always"
          env {
            name  = "KAFKA_BOOTSTRAP"
            value = "kafka:9092"
          }
          env {
            name  = "KAFKA_TOPIC"
            value = "events"
          }
          env {
            name  = "OUTPUT_DIR"
            value = "/data/output"
          }
          volume_mount {
            name       = "output"
            mount_path = "/data/output"
          }
          resources {
            requests = {
              cpu    = "500m"
              memory = "2Gi"
            }
            limits = {
              memory = "4Gi"
            }
          }
        }
        volume {
          name = "output"
          empty_dir {}
        }
      }
    }
  }
  depends_on = [kubernetes_job_v1.kafka_init]
}

resource "kubernetes_deployment_v1" "client" {
  for_each = { for client in var.clients : client.id => client }

  metadata {
    name      = each.key
    namespace = kubernetes_namespace_v1.streaming.metadata[0].name
  }
  spec {
    replicas = 1
    selector {
      match_labels = {
        app       = "client"
        client-id = each.key
      }
    }
    template {
      metadata {
        labels = {
          app       = "client"
          client-id = each.key
        }
      }
      spec {
        init_container {
          name    = "wait-kafka"
          image   = "busybox:1.36"
          command = ["sh", "-c", "until nc -z kafka 9092; do sleep 2; done"]
        }
        container {
          name              = "client"
          image             = var.client_image
          image_pull_policy = "Always"
          env {
            name  = "INPUT_PATH"
            value = "/data/input/${each.value.csv}"
          }
          env {
            name  = "ROWS_PER_SECOND"
            value = tostring(var.rows_per_second)
          }
          env {
            name  = "KAFKA_BOOTSTRAP"
            value = "kafka:9092"
          }
          env {
            name  = "KAFKA_TOPIC"
            value = "events"
          }
          env {
            name  = "CLIENT_ID"
            value = each.key
          }
          volume_mount {
            name       = "input"
            mount_path = "/data/input"
            read_only  = true
          }
        }
        volume {
          name = "input"
          config_map {
            name = kubernetes_config_map_v1.input.metadata[0].name
          }
        }
      }
    }
  }
  depends_on = [kubernetes_deployment_v1.processor]
}
