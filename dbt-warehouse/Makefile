# dbt commands executed inside Docker on de_poc_network.
# Prerequisites: warehouse running, `cp .env.example .env`, `make build`

COMPOSE := docker compose --profile tools run --rm dbt
PROJECT_DIR := /usr/app

.PHONY: help build debug deps run run-sales run-sap run-api run-full migrate-schemas freshness snapshot test test-silver test-sales test-sap test-api docs docs-serve clean parse lint

help:
	@echo "Targets: build, deps, freshness, run, run-sales, run-sap, run-full, migrate-schemas, snapshot"
	@echo "         test, test-silver, test-sales, test-sap, parse, lint, docs, clean"
	@echo "Workflow: Airbyte sync -> make freshness -> make run-full (first time) -> make snapshot -> make test"

build:
	docker compose build dbt

debug:
	$(COMPOSE) debug --profiles-dir $(PROJECT_DIR)

deps:
	$(COMPOSE) deps --profiles-dir $(PROJECT_DIR)

run:
	$(COMPOSE) run --profiles-dir $(PROJECT_DIR) --select tag:pipeline_sales_local_postgres tag:pipeline_sap_chemicals

run-sales:
	$(COMPOSE) run --profiles-dir $(PROJECT_DIR) --select tag:pipeline_sales_local_postgres

run-sap:
	$(COMPOSE) run --profiles-dir $(PROJECT_DIR) --select tag:pipeline_sap_chemicals

run-api:
	$(COMPOSE) run --profiles-dir $(PROJECT_DIR) --select tag:pipeline_api_countries

# First run after schema migration — rebuilds all models in silver_* / gold_* schemas
run-full:
	$(COMPOSE) run --profiles-dir $(PROJECT_DIR) --full-refresh

freshness:
	$(COMPOSE) source freshness --profiles-dir $(PROJECT_DIR)

snapshot:
	$(COMPOSE) snapshot --profiles-dir $(PROJECT_DIR) --select snap_stg_customers

test:
	$(COMPOSE) test --profiles-dir $(PROJECT_DIR)

test-silver:
	$(COMPOSE) test --profiles-dir $(PROJECT_DIR) --select tag:medallion_silver
	$(COMPOSE) test --profiles-dir $(PROJECT_DIR) --select assert_no_orphan_orders_in_silver

test-sales:
	$(COMPOSE) test --profiles-dir $(PROJECT_DIR) --select tag:pipeline_sales_local_postgres source:src_local_postgres
	$(COMPOSE) test --profiles-dir $(PROJECT_DIR) --select assert_no_orphan_orders_in_silver

test-sap:
	$(COMPOSE) test --profiles-dir $(PROJECT_DIR) --select tag:pipeline_sap_chemicals source:src_sap_chemicals

parse:
	DBT_TARGET=ci $(COMPOSE) parse --profiles-dir $(PROJECT_DIR)

lint:
	@command -v sqlfluff >/dev/null 2>&1 || (echo "Install: pip install sqlfluff sqlfluff-templater-dbt" && exit 1)
	DBT_TARGET=ci sqlfluff lint models/pipelines macros --dialect postgres

docs:
	$(COMPOSE) docs generate --profiles-dir $(PROJECT_DIR)

docs-serve:
	docker compose --profile docs up dbt-docs

clean:
	rm -rf target logs
	docker compose --profile tools run --rm dbt clean --profiles-dir $(PROJECT_DIR)

# Drop legacy silver/gold schemas after run-full (see analyses/migration_per_pipeline_schemas.sql)
migrate-schemas:
	@echo "Run analyses/migration_per_pipeline_schemas.sql in DBeaver after make run-full"
