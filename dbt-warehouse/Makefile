# dbt commands executed inside Docker on de_poc_network.
# Prerequisites: warehouse running, `cp .env.example .env`, `make build`

COMPOSE := docker compose --profile tools run --rm dbt
PROJECT_DIR := /usr/app

.PHONY: help build debug deps seed run run-sales run-sap run-full freshness snapshot test test-silver test-sales test-sap docs docs-serve clean parse

help:
	@echo "Targets: build, deps, freshness, run, run-sales, run-sap, snapshot, test, test-silver, run-full, parse, docs, clean"
	@echo "Workflow: Airbyte sync -> make freshness -> make run -> make snapshot -> make test"

build:
	docker compose build dbt

debug:
	$(COMPOSE) debug --profiles-dir $(PROJECT_DIR)

deps:
	$(COMPOSE) deps --profiles-dir $(PROJECT_DIR)

seed:
	$(COMPOSE) seed --profiles-dir $(PROJECT_DIR)

# All pipelines (Silver + Gold)
run:
	$(COMPOSE) run --profiles-dir $(PROJECT_DIR) --select tag:pipeline_sales_local_postgres tag:pipeline_sap_chemicals

run-sales:
	$(COMPOSE) run --profiles-dir $(PROJECT_DIR) --select tag:pipeline_sales_local_postgres

run-sap:
	$(COMPOSE) run --profiles-dir $(PROJECT_DIR) --select tag:pipeline_sap_chemicals

freshness:
	$(COMPOSE) source freshness --profiles-dir $(PROJECT_DIR)

snapshot:
	$(COMPOSE) snapshot --profiles-dir $(PROJECT_DIR) --select snap_stg_customers

run-full:
	$(COMPOSE) run --profiles-dir $(PROJECT_DIR) --full-refresh

test:
	$(COMPOSE) test --profiles-dir $(PROJECT_DIR)

test-silver:
	$(COMPOSE) test --profiles-dir $(PROJECT_DIR) --select tag:medallion_silver
	$(COMPOSE) test --profiles-dir $(PROJECT_DIR) --select test_type:singular

test-sales:
	$(COMPOSE) test --profiles-dir $(PROJECT_DIR) --select tag:pipeline_sales_local_postgres source:src_local_postgres
	$(COMPOSE) test --profiles-dir $(PROJECT_DIR) --select assert_no_orphan_orders_in_silver

test-sap:
	$(COMPOSE) test --profiles-dir $(PROJECT_DIR) --select tag:pipeline_sap_chemicals source:src_sap_chemicals

parse:
	DBT_TARGET=ci $(COMPOSE) parse --profiles-dir $(PROJECT_DIR)

docs:
	$(COMPOSE) docs generate --profiles-dir $(PROJECT_DIR)

docs-serve:
	docker compose --profile docs up dbt-docs

clean:
	rm -rf target logs
	docker compose --profile tools run --rm dbt clean --profiles-dir $(PROJECT_DIR)
