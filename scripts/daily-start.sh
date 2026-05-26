#!/usr/bin/env bash
# Daily start: avoid DB recreation; bring services up in safe order.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "Starting Source Postgres..."
(cd source-postgres && docker compose up -d --no-recreate)

echo "Starting Warehouse Postgres..."
(cd warehouse-postgres && docker compose up -d --no-recreate)

echo "Starting Airbyte..."
(cd airbyte-platform && docker compose up -d --no-recreate)

echo "Starting Airflow..."
(cd airflow-platform && docker compose up -d --no-recreate airflow-scheduler airflow-webserver)

echo "Starting CKAN..."
(cd ckan-platform && docker compose up -d --no-recreate)

echo "Done."
