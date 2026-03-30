.PHONY: help up down logs build test clean

COMPOSE = docker compose

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ── Infrastructure ────────────────────────────────────────────────────────────

build: ## Build all Docker images
	$(COMPOSE) build

up: ## Start all services (detached)
	$(COMPOSE) up -d

down: ## Stop and remove all containers
	$(COMPOSE) down

restart: ## Restart all services
	$(COMPOSE) restart

# ── Observability ─────────────────────────────────────────────────────────────

logs: ## Tail logs for all services
	$(COMPOSE) logs -f

logs-producer: ## Tail event producer logs
	$(COMPOSE) logs -f event-producer

logs-streaming: ## Tail Spark streaming logs
	$(COMPOSE) logs -f spark-streaming

logs-batch: ## Tail batch processor logs
	$(COMPOSE) logs -f batch-processor

logs-dashboard: ## Tail dashboard logs
	$(COMPOSE) logs -f dashboard

status: ## Show container status
	$(COMPOSE) ps

# ── Kafka utilities ───────────────────────────────────────────────────────────

kafka-topics: ## List Kafka topics
	$(COMPOSE) exec kafka kafka-topics --bootstrap-server localhost:9092 --list

kafka-describe: ## Describe all Kafka topics
	$(COMPOSE) exec kafka kafka-topics --bootstrap-server localhost:9092 --describe

# ── Testing ───────────────────────────────────────────────────────────────────

install-dev: ## Install dev dependencies locally
	pip install -r requirements-dev.txt

test: ## Run unit tests
	pytest tests/ -v

test-schemas: ## Run schema tests only
	pytest tests/test_schemas.py -v

test-producer: ## Run producer tests only
	pytest tests/test_producer.py -v

test-batch: ## Run batch transformation tests only
	pytest tests/test_batch_transformations.py -v

# ── Clean-up ──────────────────────────────────────────────────────────────────

clean: ## Remove containers, volumes and images
	$(COMPOSE) down -v --rmi local
