terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.80"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.33"
    }
  }
}

variable "region" {
  type    = string
  default = "us-east-1"
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

provider "aws" {
  region = var.region
}

data "aws_availability_zones" "available" {
  state = "available"
}

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.19"

  name = var.cluster_name
  cidr = "10.0.0.0/16"

  azs             = slice(data.aws_availability_zones.available.names, 0, 2)
  private_subnets = ["10.0.1.0/24", "10.0.2.0/24"]
  public_subnets  = ["10.0.11.0/24", "10.0.12.0/24"]

  enable_nat_gateway = true
  single_nat_gateway = true
}

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.31"

  cluster_name    = var.cluster_name
  cluster_version = "1.31"

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  cluster_endpoint_public_access           = true
  enable_cluster_creator_admin_permissions = true

  eks_managed_node_groups = {
    default = {
      instance_types = ["t3.large"]
      min_size       = 1
      max_size       = 3
      desired_size   = 2
      disk_size      = 50
    }
  }
}

data "aws_eks_cluster_auth" "this" {
  name = module.eks.cluster_name
}

provider "kubernetes" {
  host                   = module.eks.cluster_endpoint
  cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)
  token                  = data.aws_eks_cluster_auth.this.token
}

resource "aws_ecr_repository" "client" {
  name                 = "${var.cluster_name}-client"
  image_tag_mutability = "MUTABLE"
  force_delete         = true
}

resource "aws_ecr_repository" "flink" {
  name                 = "${var.cluster_name}-flink"
  image_tag_mutability = "MUTABLE"
  force_delete         = true
}

resource "aws_ecr_repository" "spark" {
  name                 = "${var.cluster_name}-spark"
  image_tag_mutability = "MUTABLE"
  force_delete         = true
}

locals {
  processor_repo = var.pipeline == "spark" ? aws_ecr_repository.spark.repository_url : aws_ecr_repository.flink.repository_url
}

module "workload" {
  source = "../modules/workload"

  pipeline        = var.pipeline
  client_image    = "${aws_ecr_repository.client.repository_url}:${var.image_tag}"
  processor_image = "${local.processor_repo}:${var.image_tag}"
  rows_per_second = var.rows_per_second
  clients         = var.clients

  depends_on = [module.eks]
}

output "cluster_name" {
  value = module.eks.cluster_name
}

output "client_image" {
  value = "${aws_ecr_repository.client.repository_url}:${var.image_tag}"
}

output "flink_image" {
  value = "${aws_ecr_repository.flink.repository_url}:${var.image_tag}"
}

output "spark_image" {
  value = "${aws_ecr_repository.spark.repository_url}:${var.image_tag}"
}

output "processor_image" {
  value = "${local.processor_repo}:${var.image_tag}"
}

output "kubeconfig_command" {
  value = "aws eks update-kubeconfig --region ${var.region} --name ${module.eks.cluster_name}"
}
