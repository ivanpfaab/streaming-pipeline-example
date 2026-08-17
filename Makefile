COMPOSE_FILE := docker-compose.generated.yml
COMPOSE := docker compose -f $(COMPOSE_FILE)
CLIENT_ID ?=
CLIENT_SERVICE := $(if $(filter client-%,$(CLIENT_ID)),$(CLIENT_ID),$(if $(CLIENT_ID),client-$(CLIENT_ID),))

.PHONY: generate-compose server client down client-logs server-logs

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
