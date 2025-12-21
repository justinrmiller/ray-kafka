.PHONY: help unit integration ui up down clean logs schema connect build

help:
	@echo "Available targets:"
	@echo "  unit         - Run unit tests (fast, mocked)"
	@echo "  integration  - Run integration tests (requires Kafka)"
	@echo "  schema       - Start with Schema Registry"
	@echo "  connect      - Start with Kafka Connect"
	@echo "  up           - Start Kafka services only"
	@echo "  down         - Stop all services"
	@echo "  clean        - Stop services and remove volumes"
	@echo "  logs         - Show service logs"
	@echo "  build        - Rebuild test containers"

build:
	docker-compose build unit-tests integration-tests

up:
	docker-compose up -d kafka

down:
	docker-compose down

clean:
	docker-compose down -v
	docker system prune -f

unit:
	docker-compose --profile test run --rm unit-tests

integration: up
	@echo "Running integration tests..."
	docker-compose --profile integration run --rm integration-tests
	$(MAKE) down

schema: up
	docker-compose --profile schema up -d schema-registry
	@echo ""
	@echo "Schema Registry available at http://localhost:8081"
	@echo ""

connect: up
	docker-compose --profile connect up -d kafka-connect
	@echo ""
	@echo "Kafka Connect available at http://localhost:8083"
	@echo ""

# Format and lint
format:
	uv run ruff format .

lint:
	uv run ruff check --fix .

check:
	uv run ruff check .
	uv run ruff format --check .

# Full test suite
test-all: unit integration
	@echo "All tests passed!"
