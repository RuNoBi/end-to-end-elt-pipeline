# dbt commands executed inside Docker on de_poc_network.
# Prerequisites: warehouse running, `cp .env.example .env`, `make build`

COMPOSE := docker compose --profile tools run --rm dbt
PROJECT_DIR := /usr/app

.PHONY: help build debug deps seed run run-full freshness snapshot test test-silver docs docs-serve clean parse

help:
	@echo "Targets: build, deps, freshness, run, snapshot, test, test-silver, run-full, parse, docs, clean"
	@echo "Workflow: Airbyte sync -> make freshness -> make run -> make snapshot -> make test"

build:
	docker compose build dbt

debug:
	$(COMPOSE) debug --profiles-dir $(PROJECT_DIR)

deps:
	$(COMPOSE) deps --profiles-dir $(PROJECT_DIR)

seed:
	$(COMPOSE) seed --profiles-dir $(PROJECT_DIR)

# Incremental pipeline: silver staging → gold dims/facts/marts
run:
	$(COMPOSE) run --profiles-dir $(PROJECT_DIR) --select staging+ intermediate+ marts+

freshness:
	$(COMPOSE) source freshness --profiles-dir $(PROJECT_DIR)

snapshot:
	$(COMPOSE) snapshot --profiles-dir $(PROJECT_DIR) --select snap_stg_customers

# Full-refresh all models (use after large backfills or logic changes)
run-full:
	$(COMPOSE) run --profiles-dir $(PROJECT_DIR) --full-refresh

test:
	$(COMPOSE) test --profiles-dir $(PROJECT_DIR)

test-silver:
	$(COMPOSE) test --profiles-dir $(PROJECT_DIR) --select staging intermediate source:src_local_postgres
	$(COMPOSE) test --profiles-dir $(PROJECT_DIR) --select test_type:singular

parse:
	DBT_TARGET=ci $(COMPOSE) parse --profiles-dir $(PROJECT_DIR)

docs:
	$(COMPOSE) docs generate --profiles-dir $(PROJECT_DIR)

docs-serve:
	docker compose --profile docs up dbt-docs

clean:
	rm -rf target logs
	docker compose --profile tools run --rm dbt clean --profiles-dir $(PROJECT_DIR)
