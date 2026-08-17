terraform {
  required_version = ">= 1.5.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.14"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.33"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

variable "subscription_id" {
  type        = string
  description = "Azure subscription to create the cluster in."
}

variable "location" {
  type    = string
  default = "eastus"
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

provider "azurerm" {
  features {}
  subscription_id = var.subscription_id
}

resource "azurerm_resource_group" "this" {
  name     = var.cluster_name
  location = var.location
}

resource "random_id" "acr" {
  byte_length = 3
}

resource "azurerm_container_registry" "this" {
  name                = "${replace(var.cluster_name, "-", "")}${random_id.acr.hex}"
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  sku                 = "Basic"
  admin_enabled       = true
}

resource "azurerm_kubernetes_cluster" "this" {
  name                = var.cluster_name
  location            = azurerm_resource_group.this.location
  resource_group_name = azurerm_resource_group.this.name
  dns_prefix          = var.cluster_name

  default_node_pool {
    name       = "default"
    node_count = 2
    vm_size    = "Standard_D4s_v3"
  }

  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_role_assignment" "acr" {
  principal_id                     = azurerm_kubernetes_cluster.this.kubelet_identity[0].object_id
  role_definition_name             = "AcrPull"
  scope                            = azurerm_container_registry.this.id
  skip_service_principal_aad_check = true
}

provider "kubernetes" {
  host                   = azurerm_kubernetes_cluster.this.kube_config[0].host
  client_certificate     = base64decode(azurerm_kubernetes_cluster.this.kube_config[0].client_certificate)
  client_key             = base64decode(azurerm_kubernetes_cluster.this.kube_config[0].client_key)
  cluster_ca_certificate = base64decode(azurerm_kubernetes_cluster.this.kube_config[0].cluster_ca_certificate)
}

locals {
  processor_name = var.pipeline == "spark" ? "spark" : "flink"
}

module "workload" {
  source = "../modules/workload"

  pipeline        = var.pipeline
  client_image    = "${azurerm_container_registry.this.login_server}/streaming-client:${var.image_tag}"
  processor_image = "${azurerm_container_registry.this.login_server}/streaming-${local.processor_name}:${var.image_tag}"
  rows_per_second = var.rows_per_second
  clients         = var.clients

  depends_on = [azurerm_kubernetes_cluster.this, azurerm_role_assignment.acr]
}

output "resource_group" {
  value = azurerm_resource_group.this.name
}

output "acr_login_server" {
  value = azurerm_container_registry.this.login_server
}

output "client_image" {
  value = "${azurerm_container_registry.this.login_server}/streaming-client:${var.image_tag}"
}

output "processor_image" {
  value = "${azurerm_container_registry.this.login_server}/streaming-${local.processor_name}:${var.image_tag}"
}

output "kubeconfig_command" {
  value = "az aks get-credentials --resource-group ${azurerm_resource_group.this.name} --name ${azurerm_kubernetes_cluster.this.name}"
}
