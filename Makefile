
SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c

.DEFAULT_GOAL := help

RATE ?= 500
DURATION ?= 60

MALFORMED_RATE ?= 0.0
DUP_RATE ?= 0.0
JOB_ARGS ?=
SAVEPOINT ?=
LIMIT ?= 10
DEMO_DURATION ?= 120
CHAOS_DRILLS ?= taskmanager kafka writer dynamodb
MINUTES ?= 5
TOPIC ?=
RATES ?= 50 100 150 200 300 500 2000
BENCH_DURATION ?= 120
SEED ?= 42
PARALLELISM ?= 1
PARTITIONS ?= 6
# The AWS CLI profile for make tf-plan, deploy and destroy (aws login --profile rp).
AWS_PROFILE ?= rp
# Extra Terraform arguments, e.g. TF_ARGS=-auto-approve.
TF_ARGS ?=

# Every host, topic and table is in .env (documented in .env.example). The
# Python tools and the Flink containers read it themselves, so make never
# includes it and a variable set in the shell still wins. make install and
# make up create it from .env.example if it is missing.
.env:
	cp .env.example .env

.PHONY: help install fmt lint typecheck test test-flink check hooks generate up down topics logs produce test-int submit-job stop-job check-trending db-init db-smoke writer check-writer api-local demo test-e2e chaos metrics bench bench-report package-lambda tf-check tf-plan deploy destroy

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
install: .env
	poetry install --with flink

## Format Python code with Ruff.
fmt:
	poetry run ruff format .

## Check Python code style with Ruff.
lint:
	poetry run ruff check .

## Run static type checking with mypy.
typecheck:
	poetry run mypy src jobs

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
up: .env
	docker compose up -d

## Stop the local Kafka stack.
down:
	docker compose down

## Stop the local Kafka stack and delete kafka data.
down_delete:
	docker compose down -v

## Create the Kafka topics named in .env.
topics:
	poetry run python -m realtime_recommendation_pipeline.tools.create_topics

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
	docker compose exec -T flink-jobmanager sh -c "flink stop --savepointPath \$$FLINK_CHECKPOINT_DIR/savepoints $$job_id"

## Check trending-results for gaps and duplicates, reading only committed rows.
check-trending:
	poetry run python -m realtime_recommendation_pipeline.tools.check_trending

## Create the DynamoDB Local table from infra/dynamodb/trending-table.json.
db-init:
	poetry run python -m realtime_recommendation_pipeline.tools.db_init \
		--definition infra/dynamodb/trending-table.json

## Write sample windows to DynamoDB Local and read the latest back.
db-smoke:
	poetry run python -m realtime_recommendation_pipeline.tools.db_smoke

## Write trending-results windows to DynamoDB Local (Ctrl-C stops it cleanly).
writer:
	poetry run python -m realtime_recommendation_pipeline.writer.results_writer

## Check the table holds one correct item per window on trending-results.
check-writer:
	poetry run python -m realtime_recommendation_pipeline.tools.check_writer

## Call the API's Lambda handler against DynamoDB Local (GET /trending?limit=LIMIT).
api-local:
	poetry run python -m realtime_recommendation_pipeline.tools.api_local \
		--limit $(LIMIT)

## Run the whole pipeline on demo topics with 2-minute windows and print the trending list.
demo: up
	poetry run python -m realtime_recommendation_pipeline.tools.demo \
		--duration $(DEMO_DURATION)

## End-to-end test on the local stack: demo results reach DynamoDB in the agreed time.
test-e2e: up
	poetry run pytest -m e2e -s

## Run the failure drills on the local stack and record the results in docs/reliability.md.
chaos: up
	poetry run python -m realtime_recommendation_pipeline.tools.chaos \
		$(foreach drill,$(CHAOS_DRILLS),--drill $(drill))

## Print throughput, consumer lag and latency p50/p95/p99 for the last MINUTES (default 5); TOPIC=viewing-events-demo during a demo.
metrics:
	poetry run python -m realtime_recommendation_pipeline.tools.metrics_report \
		--minutes $(MINUTES) $(if $(TOPIC),--topic $(TOPIC))

## Benchmark live runs at each of RATES events/s for BENCH_DURATION s each (PARALLELISM, PARTITIONS, SEED) into docs/benchmarks/.
bench: up
	poetry run python -m realtime_recommendation_pipeline.tools.bench \
		--rates $(RATES) --duration $(BENCH_DURATION) --seed $(SEED) \
		--parallelism $(PARALLELISM) --partitions $(PARTITIONS)

## Redraw the benchmark charts and print the results table from docs/benchmarks/.
bench-report:
	poetry run python -m realtime_recommendation_pipeline.tools.bench --report-only

# --- AWS (infra/terraform) ----------------------------------------------------

LAMBDA_BUILD := build/lambda
# The Lambda runtime is the project's Python version (infra/terraform/lambda.tf).
LAMBDA_PYTHON := $(shell cat .python-version)
TF := terraform -chdir=infra/terraform
# Terraform gets AWS_PROFILE's short-lived credentials from the AWS CLI, which
# handles aws login and SSO profiles alike. AWS_PROFILE is then unset, because
# Terraform would read the profile itself instead of the credentials.
WITH_AWS := credentials=$$(aws configure export-credentials --profile $(AWS_PROFILE) --format env) \
	&& eval "$$credentials" && unset AWS_PROFILE &&

## Build the API's Lambda package, build/lambda.zip: the handler's modules and the lambda dependency group.
# Wheels are for the runtime: Linux x86_64, glibc 2.17 or older, the project's
# Python. Fixed timestamps and file order make the same code give the same zip,
# so Terraform only updates the function when something changed.
package-lambda:
	rm -rf $(LAMBDA_BUILD) build/lambda.zip
	mkdir -p $(LAMBDA_BUILD)/realtime_recommendation_pipeline
	poetry export --only lambda --format requirements.txt --output build/lambda-requirements.txt
	poetry run python -m pip install --quiet --disable-pip-version-check --no-compile --target $(LAMBDA_BUILD) \
		--requirement build/lambda-requirements.txt \
		--platform manylinux2014_x86_64 --implementation cp \
		--python-version $(LAMBDA_PYTHON) --only-binary=:all:
	cp -R src/realtime_recommendation_pipeline/{__init__.py,settings.py,api,common} \
		$(LAMBDA_BUILD)/realtime_recommendation_pipeline/
	find $(LAMBDA_BUILD) -name __pycache__ -prune -exec rm -rf {} +
	find $(LAMBDA_BUILD) -exec touch -t 202001010000 {} +
	cd $(LAMBDA_BUILD) && find . -type f | LC_ALL=C sort | zip -q -X ../lambda.zip -@

## Check the Terraform is formatted and valid, without AWS access (CI runs this).
tf-check:
	$(TF) fmt -check -recursive
	$(TF) init -backend=false -input=false
	$(TF) validate

## Show what make deploy would change in AWS (AWS_PROFILE, default rp; run aws login first).
tf-plan: package-lambda
	$(WITH_AWS) $(TF) init -input=false && $(TF) plan $(TF_ARGS)

## Create or update the AWS stack: table, Lambda, HTTP API, logs and IAM roles (asks first).
deploy: package-lambda
	$(WITH_AWS) $(TF) init -input=false && $(TF) apply $(TF_ARGS)

## Delete everything make deploy created (asks first).
destroy: package-lambda
	$(WITH_AWS) $(TF) init -input=false && $(TF) destroy $(TF_ARGS)
