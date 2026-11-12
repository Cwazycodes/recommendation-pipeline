
SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c

.DEFAULT_GOAL := help

RATE ?= 500
DURATION ?= 60

MALFORMED_RATE ?= 0.0
DUP_RATE ?= 0.0
# Stamp produced events with the time they are sent (make produce LIVE=1).
LIVE ?=
# The event schema version make produce writes; empty means the newest.
SCHEMA_VERSION ?=
# The git ref whose event schemas make schema-check treats as released.
SCHEMA_BASE ?= origin/main
JOB_ARGS ?=
SAVEPOINT ?=
LIMIT ?= 10
DEMO_DURATION ?= 120
CHAOS_DRILLS ?= taskmanager kafka writer dynamodb continue-watching schema
MINUTES ?= 5
TOPIC ?=
RATES ?= 50 100 150 200 300 500 2000
BENCH_DURATION ?= 120
SEED ?= 42
PARALLELISM ?= 1
PARTITIONS ?= 6
# Where make bench saves its results, and make bench-aws its own.
BENCH_DIR ?= docs/benchmarks
BENCH_AWS_DIR ?= docs/benchmarks/dynamodb/aws
# The one rate make bench-aws runs, events/s.
AWS_BENCH_RATE ?= 100
# make cw-bench: the continue-watching job's rates, events/s, and its results.
CW_RATES ?= 100
CW_BENCH_DIR ?= docs/benchmarks/continue-watching
# The user make cw-api-local asks about.
USER_ID ?= user-00040
# make stop-job stops the first running job whose name contains this.
JOB_NAME ?=
# The AWS CLI profile for make tf-plan, deploy and destroy (aws login --profile rp).
AWS_PROFILE ?= rp
# The results writer's profile, which assumes its role (infra/terraform/writer.tf).
AWS_WRITER_PROFILE ?= rp-writer
# Extra Terraform arguments, e.g. TF_ARGS=-auto-approve.
TF_ARGS ?=

# Every host, topic and table is in .env (documented in .env.example). The
# Python tools and the Flink containers read it themselves, so make never
# includes it and a variable set in the shell still wins. make install and
# make up create it from .env.example if it is missing.
.env:
	cp .env.example .env

.PHONY: help install fmt lint typecheck test test-flink check hooks generate up down topics schema-check logs produce test-int test-registry submit-job stop-job check-trending db-init db-smoke writer check-writer api-local cw-writer cw-writer-aws cw-api-local demo test-e2e chaos metrics bench bench-report writer-aws bench-aws cw-bench package-lambda tf-check tf-plan deploy destroy bootstrap api-url

## Show all available targets and their descriptions.
help:
	@awk '/^## / { description = substr($$0, 4); next } \
		/^[a-zA-Z0-9_-]+:/ { \
			split($$0, target, ":"); \
			if (description != "") \
				printf "  make %-16s %s\n", target[1], description; \
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
	poetry run pytest -m "not integration and not slow and not e2e and not registry" --cov=realtime_recommendation_pipeline --cov-report=term-missing --cov-report=xml:coverage.xml

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

## Check the event schemas against the released ones (SCHEMA_BASE) with the stack's Schema Registry.
schema-check:
	poetry run python -m realtime_recommendation_pipeline.tools.schema_check --base-ref $(SCHEMA_BASE)

## Follow logs from the Kafka containers.
logs:
	docker compose logs -f kafka kafka-ui

## Produce synthetic events to local Kafka (LIVE=1 stamps them with the time they are sent; SCHEMA_VERSION=1 writes v1).
produce:
	poetry run python -m realtime_recommendation_pipeline.producer.kafka_producer \
		--rate $(RATE) \
		--duration $(DURATION) \
		--malformed-rate $(MALFORMED_RATE) \
		--dup-rate $(DUP_RATE) \
		$(if $(SCHEMA_VERSION),--schema-version $(SCHEMA_VERSION)) \
		$(if $(LIVE),--live)

## Run integration tests (Kafka and DynamoDB Local in Docker).
test-int:
	TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock poetry run pytest -m integration

## Run the tests that need the stack's Schema Registry (make up first).
test-registry:
	poetry run pytest -m registry tests/registry

## Submit a PyFlink job from jobs/ (e.g. make submit-job JOB=validate_events).
submit-job:
	@if [ -z "$(JOB)" ]; then echo "Usage: make submit-job JOB=smoke"; exit 1; fi
	docker compose exec flink-jobmanager flink run -d \
		$(if $(SAVEPOINT),--fromSavepoint $(SAVEPOINT)) \
		-py /opt/flink/jobs/$(JOB).py \
		-pyfs /opt/flink/src \
		$(JOB_ARGS)

## Stop a running job with a savepoint to the checkpoint volume (JOB_NAME=continue-watching picks which).
stop-job:
	@job_id=$$(docker compose exec -T flink-jobmanager flink list -r 2>/dev/null | awk -F ' : ' '/RUNNING/ && /$(JOB_NAME)/ {print $$2; exit}'); \
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

## Write trending-results windows to DynamoDB in AWS instead, as the writer's role (AWS_WRITER_PROFILE).
writer-aws:
	DYNAMODB_ENDPOINT_URL= AWS_PROFILE=$(AWS_WRITER_PROFILE) \
		poetry run python -m realtime_recommendation_pipeline.writer.results_writer

## Check the table holds one correct item per window on trending-results.
check-writer:
	poetry run python -m realtime_recommendation_pipeline.tools.check_writer

## Call the API's Lambda handler against DynamoDB Local (GET /trending?limit=LIMIT).
api-local:
	poetry run python -m realtime_recommendation_pipeline.tools.api_local \
		--limit $(LIMIT)

## Write continue-watching changes to DynamoDB Local (Ctrl-C stops it cleanly).
cw-writer:
	poetry run python -m realtime_recommendation_pipeline.writer.continue_watching_writer

## Write continue-watching changes to DynamoDB in AWS instead, as the writer's role (AWS_WRITER_PROFILE).
cw-writer-aws:
	DYNAMODB_ENDPOINT_URL= AWS_PROFILE=$(AWS_WRITER_PROFILE) \
		poetry run python -m realtime_recommendation_pipeline.writer.continue_watching_writer

## Call the API's Lambda handler for USER_ID's continue-watching list against DynamoDB Local.
cw-api-local:
	poetry run python -m realtime_recommendation_pipeline.tools.api_local \
		--user $(USER_ID) --limit $(LIMIT)

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

## Benchmark live runs at each of RATES events/s for BENCH_DURATION s each (PARALLELISM, PARTITIONS, SEED) into BENCH_DIR.
bench: up
	poetry run python -m realtime_recommendation_pipeline.tools.bench \
		--rates $(RATES) --duration $(BENCH_DURATION) --seed $(SEED) \
		--parallelism $(PARALLELISM) --partitions $(PARTITIONS) --out-dir $(BENCH_DIR)

## Redraw the benchmark charts and print the results table from BENCH_DIR (default docs/benchmarks/).
bench-report:
	poetry run python -m realtime_recommendation_pipeline.tools.bench --report-only --out-dir $(BENCH_DIR)

## Benchmark one run at AWS_BENCH_RATE events/s with the writer on DynamoDB in AWS (AWS_WRITER_PROFILE), into BENCH_AWS_DIR.
bench-aws: up
	DYNAMODB_ENDPOINT_URL= AWS_PROFILE=$(AWS_WRITER_PROFILE) \
		poetry run python -m realtime_recommendation_pipeline.tools.bench \
		--rates $(AWS_BENCH_RATE) --duration $(BENCH_DURATION) --seed $(SEED) \
		--parallelism $(PARALLELISM) --partitions $(PARTITIONS) --out-dir $(BENCH_AWS_DIR)

## Benchmark the continue-watching job at each of CW_RATES events/s for BENCH_DURATION s into CW_BENCH_DIR.
cw-bench: up
	poetry run python -m realtime_recommendation_pipeline.tools.bench_continue_watching \
		--rates $(CW_RATES) --duration $(BENCH_DURATION) --seed $(SEED) \
		--parallelism $(PARALLELISM) --partitions $(PARTITIONS) --out-dir $(CW_BENCH_DIR)


# --- AWS (infra/terraform) ----------------------------------------------------

LAMBDA_BUILD := build/lambda
# The Lambda runtime is the project's Python version (infra/terraform/lambda.tf).
LAMBDA_PYTHON := $(shell cat .python-version)
TF := terraform -chdir=infra/terraform
BOOTSTRAP := terraform -chdir=infra/bootstrap
# Terraform gets AWS_PROFILE's short-lived credentials from the AWS CLI, which
# handles aws login and SSO profiles alike. With AWS_PROFILE empty, as in the
# deploy workflow, they are already in the environment. AWS_PROFILE is then
# unset, because Terraform would read the profile itself instead.
WITH_AWS := $(if $(AWS_PROFILE),credentials=$$(aws configure export-credentials --profile $(AWS_PROFILE) --format env) \
	&& eval "$$credentials" &&) unset AWS_PROFILE &&
# The state bucket make bootstrap created, named after the account
# (infra/bootstrap/variables.tf).
TF_INIT := account=$$(aws sts get-caller-identity --query Account --output text) \
	&& $(TF) init -input=false -backend-config="bucket=recommendation-pipeline-tfstate-$$account"

## Build the API's Lambda package, build/lambda.zip: the handler's modules and the lambda dependency group.
# Wheels are for the runtime: Linux x86_64, glibc 2.17 or older, the project's
# Python. Fixed timestamps and file order make the same code give the same zip,
# so Terraform only updates the function when something changed. Console
# scripts (bin/) name the building machine's Python, so they and the RECORD
# files that list them are left out: a laptop and CI build the same zip.
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
	rm -rf $(LAMBDA_BUILD)/bin
	find $(LAMBDA_BUILD) -name __pycache__ -prune -exec rm -rf {} +
	find $(LAMBDA_BUILD) -path '*.dist-info/RECORD' -delete
	find $(LAMBDA_BUILD) -exec touch -t 202001010000 {} +
	cd $(LAMBDA_BUILD) && find . -type f | LC_ALL=C sort | zip -q -X ../lambda.zip -@

## Check the Terraform is formatted and valid, without AWS access (CI runs this).
# validate needs the providers: a fresh checkout, as in CI, installs them with
# init -backend=false. Where make deploy has already run, they are installed,
# and init would read the state in S3, which needs AWS credentials.
tf-check:
	terraform fmt -check -recursive infra
	test -d infra/terraform/.terraform || $(TF) init -backend=false -input=false
	$(TF) validate
	test -d infra/bootstrap/.terraform || $(BOOTSTRAP) init -backend=false -input=false
	$(BOOTSTRAP) validate

## Show what make deploy would change in AWS (AWS_PROFILE, default rp; run aws login first).
tf-plan: package-lambda
	$(WITH_AWS) $(TF_INIT) && $(TF) plan $(TF_ARGS)

## Create or update the AWS stack: table, Lambda, HTTP API, logs and IAM roles (asks first).
deploy: package-lambda
	$(WITH_AWS) $(TF_INIT) && $(TF) apply $(TF_ARGS)

## Delete everything make deploy created (asks first).
destroy: package-lambda
	$(WITH_AWS) $(TF_INIT) && $(TF) destroy $(TF_ARGS)

## Print the deployed API's URL, e.g. curl "$(make -s api-url)"; it reads the S3 state, so it uses AWS_PROFILE too.
api-url:
	@$(WITH_AWS) $(TF_INIT) >/dev/null && $(TF) output -raw api_url && echo

## Create the state bucket and GitHub's deploy role, once, with your own profile (infra/bootstrap; asks first).
bootstrap:
	$(WITH_AWS) $(BOOTSTRAP) init -input=false && $(BOOTSTRAP) apply $(TF_ARGS)
