COMPOSE_FILE := docker-compose.generated.yml
COMPOSE := docker compose -f $(COMPOSE_FILE)
CLIENT_ID ?=
CLIENT_SERVICE := $(if $(filter client-%,$(CLIENT_ID)),$(CLIENT_ID),$(if $(CLIENT_ID),client-$(CLIENT_ID),))

PIPELINE ?= flink
IMAGE_TAG ?= latest
TF_AUTO_APPROVE ?= -auto-approve
TF_VARS := -var='pipeline=$(PIPELINE)' -var='image_tag=$(IMAGE_TAG)'

.PHONY: generate-compose server client down client-logs server-logs \
	images push-cloud deploy-aws deploy-azure deploy-gcp \
	destroy-aws destroy-azure destroy-gcp

generate-compose:
	python3 scripts/generate-compose.py

server: $(COMPOSE_FILE)
	$(COMPOSE) --profile server up --build

client: $(COMPOSE_FILE)
	$(COMPOSE) --profile client up --build

down: $(COMPOSE_FILE)
	$(COMPOSE) --profile server --profile client down

client-logs: $(COMPOSE_FILE)
	@if [ -z "$(CLIENT_ID)" ]; then \
		echo "CLIENT_ID is required, e.g. make client-logs CLIENT_ID=client-1"; \
		exit 1; \
	fi
	$(COMPOSE) --profile client logs -f --tail=100 $(CLIENT_SERVICE)

server-logs: $(COMPOSE_FILE)
	$(COMPOSE) --profile server logs -f --tail=100 \
		$$(sed -n 's/^\(flink\|spark\):/\1/p' $(COMPOSE_FILE))

$(COMPOSE_FILE):
	@echo "Missing $(COMPOSE_FILE). Run: make generate-compose"
	@exit 1

# --- Cloud (Terraform) -------------------------------------------------------
# Requires terraform/<cloud>/terraform.tfvars (copy from terraform.tfvars.example).
# PIPELINE and IMAGE_TAG are passed as Terraform -var flags (override tfvars).
#
#   make deploy-azure
#   make deploy-aws PIPELINE=spark
#   make destroy-gcp

images:
	@if [ "$(PIPELINE)" != "flink" ] && [ "$(PIPELINE)" != "spark" ]; then \
		echo "PIPELINE must be flink or spark (got '$(PIPELINE)')"; \
		exit 1; \
	fi
	docker build -f client/Dockerfile -t streaming-client:$(IMAGE_TAG) .
	docker build -f server/$(PIPELINE)/Dockerfile -t streaming-$(PIPELINE):$(IMAGE_TAG) .

define require_tfvars
	@if [ ! -f terraform/$(1)/terraform.tfvars ]; then \
		echo "Missing terraform/$(1)/terraform.tfvars"; \
		echo "Copy terraform/$(1)/terraform.tfvars.example and fill in required values."; \
		exit 1; \
	fi
endef

push-cloud:
	@if [ -z "$(CLOUD)" ]; then echo "CLOUD is required"; exit 1; fi
	@set -e; \
	CLIENT_IMAGE=$$(terraform -chdir=terraform/$(CLOUD) output -raw client_image); \
	PROCESSOR_IMAGE=$$(terraform -chdir=terraform/$(CLOUD) output -raw processor_image); \
	docker tag streaming-client:$(IMAGE_TAG) $$CLIENT_IMAGE; \
	docker tag streaming-$(PIPELINE):$(IMAGE_TAG) $$PROCESSOR_IMAGE; \
	docker push $$CLIENT_IMAGE; \
	docker push $$PROCESSOR_IMAGE

deploy-aws:
	$(call require_tfvars,aws)
	terraform -chdir=terraform/aws init -input=false
	terraform -chdir=terraform/aws apply -input=false $(TF_AUTO_APPROVE) $(TF_VARS) \
		-target=module.vpc \
		-target=module.eks \
		-target=aws_ecr_repository.client \
		-target=aws_ecr_repository.flink \
		-target=aws_ecr_repository.spark
	$(MAKE) images
	@set -e; \
	CLIENT_IMAGE=$$(terraform -chdir=terraform/aws output -raw client_image); \
	ECR_HOST=$${CLIENT_IMAGE%%/*}; \
	AWS_REGION=$$(echo $$ECR_HOST | cut -d. -f4); \
	aws ecr get-login-password --region $$AWS_REGION | docker login --username AWS --password-stdin $$ECR_HOST
	$(MAKE) push-cloud CLOUD=aws
	terraform -chdir=terraform/aws apply -input=false $(TF_AUTO_APPROVE) $(TF_VARS)
	@echo "Configure kubectl:"; terraform -chdir=terraform/aws output -raw kubeconfig_command; echo

deploy-azure:
	$(call require_tfvars,azure)
	terraform -chdir=terraform/azure init -input=false
	terraform -chdir=terraform/azure apply -input=false $(TF_AUTO_APPROVE) $(TF_VARS) \
		-target=azurerm_resource_group.this \
		-target=azurerm_container_registry.this \
		-target=azurerm_kubernetes_cluster.this \
		-target=azurerm_role_assignment.acr
	$(MAKE) images
	@az acr login --name "$$(terraform -chdir=terraform/azure output -raw acr_login_server | cut -d. -f1)"
	$(MAKE) push-cloud CLOUD=azure
	terraform -chdir=terraform/azure apply -input=false $(TF_AUTO_APPROVE) $(TF_VARS)
	@echo "Configure kubectl:"; terraform -chdir=terraform/azure output -raw kubeconfig_command; echo

deploy-gcp:
	$(call require_tfvars,gcp)
	terraform -chdir=terraform/gcp init -input=false
	terraform -chdir=terraform/gcp apply -input=false $(TF_AUTO_APPROVE) $(TF_VARS) \
		-target=google_project_service.container \
		-target=google_project_service.artifactregistry \
		-target=google_artifact_registry_repository.this \
		-target=google_artifact_registry_repository_iam_member.nodes \
		-target=google_container_cluster.this
	$(MAKE) images
	@set -e; \
	REGISTRY=$$(terraform -chdir=terraform/gcp output -raw registry); \
	gcloud auth configure-docker "$${REGISTRY%%/*}" --quiet
	$(MAKE) push-cloud CLOUD=gcp
	terraform -chdir=terraform/gcp apply -input=false $(TF_AUTO_APPROVE) $(TF_VARS)
	@echo "Configure kubectl:"; terraform -chdir=terraform/gcp output -raw kubeconfig_command; echo

destroy-aws:
	$(call require_tfvars,aws)
	terraform -chdir=terraform/aws destroy -input=false $(TF_AUTO_APPROVE) $(TF_VARS)

destroy-azure:
	$(call require_tfvars,azure)
	terraform -chdir=terraform/azure destroy -input=false $(TF_AUTO_APPROVE) $(TF_VARS)

destroy-gcp:
	$(call require_tfvars,gcp)
	terraform -chdir=terraform/gcp destroy -input=false $(TF_AUTO_APPROVE) $(TF_VARS)
