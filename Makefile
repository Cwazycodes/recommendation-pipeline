
SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c

.DEFAULT_GOAL := help

.PHONY: help install fmt lint typecheck test check hooks

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

## Run automated tests with pytest and coverage.
test:
	poetry run pytest --cov=realtime_recommendation_pipeline --cov-report=term-missing

## Run linting, type checking, and tests.
check: lint typecheck test

## Install Git pre-commit hooks.
hooks:
	poetry run pre-commit install
