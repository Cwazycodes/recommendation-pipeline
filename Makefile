
SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c

.DEFAULT_GOAL := help

RATE ?= 500
DURATION ?= 60

MALFORMED_RATE ?= 0.0

.PHONY: help install fmt lint typecheck test check hooks generate up down topics logs produce test-int

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
	poetry install

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
	poetry run pytest -m "not integration" --cov=realtime_recommendation_pipeline --cov-report=term-missing --cov-report=xml:coverage.xml

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
		--duration $(DURATION)\
		--malformed-rate $(MALFORMED_RATE)

## Run Kafka integration tests.
test-int:
	poetry run pytest -m integration

## Submit a PyFlink job from jobs/ (e.g. make submit-job JOB=validate_events).
submit-job:
	@if [ -z "$(JOB)" ]; then echo "Usage: make submit-job JOB=smoke"; exit 1; fi
	docker compose exec flink-jobmanager flink run -d \
		-py /opt/flink/jobs/$(JOB).py \
		-pyfs /opt/flink/src
