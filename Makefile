COMPOSE_FILE := docker-compose.generated.yml
COMPOSE := docker compose -f $(COMPOSE_FILE)

.PHONY: generate-compose server client down

generate-compose:
	python3 scripts/generate-compose.py

server: $(COMPOSE_FILE)
	$(COMPOSE) --profile server up --build

client: $(COMPOSE_FILE)
	$(COMPOSE) --profile client up --build

down: $(COMPOSE_FILE)
	$(COMPOSE) --profile server --profile client down

$(COMPOSE_FILE):
	@echo "Missing $(COMPOSE_FILE). Run: make generate-compose"
	@exit 1
