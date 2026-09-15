.DEFAULT_GOAL := help
PY ?= .venv/bin/python
UV ?= uv

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

install: ## Create .venv and install the package with dev extras
	$(UV) venv -p 3.12 .venv
	$(UV) pip install -e ".[dev]"

install-airflow: ## Separate venv with Airflow (pinned constraints) for DAG tests and `airflow dags test`
	$(UV) venv -p 3.12 .venv-airflow
	VIRTUAL_ENV=$(PWD)/.venv-airflow $(UV) pip install "apache-airflow==2.10.5" --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt
	VIRTUAL_ENV=$(PWD)/.venv-airflow $(UV) pip install -e . pytest respx hypothesis

lint: ## ruff (lint + format check) and mypy --strict
	.venv/bin/ruff check src tests airflow/dags
	.venv/bin/ruff format --check src tests airflow/dags
	.venv/bin/mypy

fmt: ## Format and autofix
	.venv/bin/ruff format src tests airflow/dags
	.venv/bin/ruff check --fix src tests airflow/dags

test: ## Unit + integration tests (Postgres test runs if GRIDCARBON_POSTGRES_DSN is set)
	$(PY) -m pytest -q

test-cov: ## Tests with coverage report
	$(PY) -m pytest -q --cov=gridcarbon --cov-report=term-missing

test-dags: ## DAG integrity tests in the Airflow venv
	.venv-airflow/bin/python -m pytest tests/integration/test_dags.py -q

run: ## Run the full pipeline for yesterday
	.venv/bin/gridcarbon run

backfill: ## make backfill START=2026-08-01 END=2026-08-31
	.venv/bin/gridcarbon backfill $(START) $(END)

build: ## Refresh warehouse views and dbt build
	.venv/bin/gridcarbon build

dbt-docs: ## Generate and serve dbt docs
	cd dbt && GRIDCARBON_DUCKDB_PATH=$(PWD)/data/warehouse/gridcarbon.duckdb ../.venv/bin/dbt docs generate --profiles-dir . && GRIDCARBON_DUCKDB_PATH=$(PWD)/data/warehouse/gridcarbon.duckdb ../.venv/bin/dbt docs serve --profiles-dir .

serve: ## Start the read API on :8000
	.venv/bin/gridcarbon serve --reload

up: ## docker compose up (Postgres, Airflow, API)
	docker compose up -d --build

down: ## docker compose down
	docker compose down

clean: ## Remove build artefacts (keeps data/)
	rm -rf dbt/target dbt/logs .pytest_cache .mypy_cache .ruff_cache
	find . -name __pycache__ -type d -prune -exec rm -rf {} +

.PHONY: help install install-airflow lint fmt test test-cov test-dags run backfill build dbt-docs serve up down clean
