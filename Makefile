
SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c

.DEFAULT_GOAL := help

RATE ?= 500
DURATION ?= 60

MALFORMED_RATE ?= 0.0
DUP_RATE ?= 0.0
JOB_ARGS ?=
SAVEPOINT ?=
DYNAMODB_ENDPOINT ?= http://localhost:8001
LIMIT ?= 10
KAFKA_BOOTSTRAP ?= localhost:9092
FLINK_URL ?= http://localhost:8081
DEMO_DURATION ?= 120
CHAOS_DRILLS ?= taskmanager kafka writer dynamodb

.PHONY: help install fmt lint typecheck test test-flink check hooks generate up down topics logs produce test-int submit-job stop-job check-trending db-init db-smoke writer check-writer api-local demo test-e2e chaos

## Show all available targets and their descriptions.
help:
	@awk 'BEGIN {FS = ":.*## "} \
		/^## / { description = substr($$0, 4); next } \
		/^[a-zA-Z0-9_-]+:.*$$/ { \
			if (description != "") \
				printf "  make %-12s %s\n", $$1, description; \
			description = "" \
		}' $(MAKEFILE_LIST)

## Install project and default development dependencies.
install:
	poetry install --with flink

## Format Python code with Ruff.
fmt:
	poetry run ruff format .

## Check Python code style with Ruff.
lint:
	poetry run ruff check .

## Run static type checking with mypy.
typecheck:
	poetry run mypy src

## Run automated tests and generate terminal and XML coverage reports.
test:
	poetry run pytest -m "not integration and not slow and not e2e" --cov=realtime_recommendation_pipeline --cov-report=term-missing --cov-report=xml:coverage.xml

## Run the slow tests that start a local Flink mini-cluster (needs Java).
test-flink:
	poetry run pytest -m slow

## Run linting, type checking, and tests.
check: lint typecheck test

## Install Git pre-commit hooks.
hooks:
	poetry run pre-commit install

## Generate synthetic viewing events.
generate:
	poetry run python -m realtime_recommendation_pipeline.producer.generate

## Start the local Kafka stack.
up:
	docker compose up -d

## Stop the local Kafka stack.
down:
	docker compose down

## Stop the local Kafka stack and delete kafka data.
down_delete:
	docker compose down -v

## Create the required Kafka topics.
topics:
	./scripts/create-topics.sh

## Follow logs from the Kafka containers.
logs:
	docker compose logs -f kafka kafka-ui

## Produce synthetic events to local Kafka.
produce:
	poetry run python -m realtime_recommendation_pipeline.producer.kafka_producer \
		--rate $(RATE) \
		--duration $(DURATION) \
		--malformed-rate $(MALFORMED_RATE) \
		--dup-rate $(DUP_RATE)

## Run integration tests (Kafka and DynamoDB Local in Docker).
test-int:
	TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock poetry run pytest -m integration

## Submit a PyFlink job from jobs/ (e.g. make submit-job JOB=validate_events).
submit-job:
	@if [ -z "$(JOB)" ]; then echo "Usage: make submit-job JOB=smoke"; exit 1; fi
	docker compose exec flink-jobmanager flink run -d \
		$(if $(SAVEPOINT),--fromSavepoint $(SAVEPOINT)) \
		-py /opt/flink/jobs/$(JOB).py \
		-pyfs /opt/flink/src \
		$(JOB_ARGS)

## Stop the running job and write a savepoint to the checkpoint volume.
stop-job:
	@job_id=$$(docker compose exec -T flink-jobmanager flink list -r 2>/dev/null | awk -F ' : ' '/RUNNING/ {print $$2; exit}'); \
	if [ -z "$$job_id" ]; then echo "No running job to stop."; exit 1; fi; \
	docker compose exec -T flink-jobmanager flink stop --savepointPath file:///opt/flink/checkpoints/savepoints "$$job_id"

## Check trending-results for gaps and duplicates, reading only committed rows.
check-trending:
	poetry run python -m realtime_recommendation_pipeline.tools.check_trending

## Create the DynamoDB Local table from infra/dynamodb/trending-table.json.
db-init:
	poetry run python -m realtime_recommendation_pipeline.tools.db_init \
		--endpoint-url $(DYNAMODB_ENDPOINT) \
		--definition infra/dynamodb/trending-table.json

## Write sample windows to DynamoDB Local and read the latest back.
db-smoke:
	poetry run python -m realtime_recommendation_pipeline.tools.db_smoke \
		--endpoint-url $(DYNAMODB_ENDPOINT)

## Write trending-results windows to DynamoDB Local (Ctrl-C stops it cleanly).
writer:
	poetry run python -m realtime_recommendation_pipeline.writer.results_writer \
		--endpoint-url $(DYNAMODB_ENDPOINT)

## Check the table holds one correct item per window on trending-results.
check-writer:
	poetry run python -m realtime_recommendation_pipeline.tools.check_writer \
		--endpoint-url $(DYNAMODB_ENDPOINT)

## Call the API's Lambda handler against DynamoDB Local (GET /trending?limit=LIMIT).
api-local:
	poetry run python -m realtime_recommendation_pipeline.tools.api_local \
		--endpoint-url $(DYNAMODB_ENDPOINT) \
		--limit $(LIMIT)

## Run the whole pipeline on demo topics with 2-minute windows and print the trending list.
demo: up
	poetry run python -m realtime_recommendation_pipeline.tools.demo \
		--bootstrap-servers $(KAFKA_BOOTSTRAP) \
		--flink-url $(FLINK_URL) \
		--endpoint-url $(DYNAMODB_ENDPOINT) \
		--duration $(DEMO_DURATION)

## End-to-end test on the local stack: demo results reach DynamoDB in the agreed time.
test-e2e: up
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) FLINK_URL=$(FLINK_URL) \
		DYNAMODB_ENDPOINT=$(DYNAMODB_ENDPOINT) poetry run pytest -m e2e -s

## Run the failure drills on the local stack and record the results in docs/reliability.md.
chaos: up
	poetry run python -m realtime_recommendation_pipeline.tools.chaos \
		--bootstrap-servers $(KAFKA_BOOTSTRAP) \
		--flink-url $(FLINK_URL) \
		--endpoint-url $(DYNAMODB_ENDPOINT) \
		$(foreach drill,$(CHAOS_DRILLS),--drill $(drill))
