"""Record Airbyte sync completion in warehouse (bronze_meta.sync_watermarks)."""

from __future__ import annotations

import logging
import os
from typing import Any

import psycopg2
from airflow.exceptions import AirflowException

logger = logging.getLogger(__name__)

_ENSURE_BRONZE_META_SQL = """
CREATE SCHEMA IF NOT EXISTS bronze_meta;
CREATE TABLE IF NOT EXISTS bronze_meta.sync_watermarks (
    bronze_schema TEXT PRIMARY KEY,
    connection_id TEXT NOT NULL,
    pipeline_id TEXT,
    synced_at TIMESTAMPTZ NOT NULL,
    job_id BIGINT,
    records_emitted BIGINT,
    records_committed BIGINT
);
"""

_UPSERT_WATERMARK_SQL = """
INSERT INTO bronze_meta.sync_watermarks (
    bronze_schema,
    connection_id,
    pipeline_id,
    synced_at,
    job_id,
    records_emitted,
    records_committed
) VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (bronze_schema) DO UPDATE SET
    connection_id = EXCLUDED.connection_id,
    pipeline_id = EXCLUDED.pipeline_id,
    synced_at = EXCLUDED.synced_at,
    job_id = EXCLUDED.job_id,
    records_emitted = EXCLUDED.records_emitted,
    records_committed = EXCLUDED.records_committed;
"""


def _warehouse_connect():
    required = (
        "DBT_WAREHOUSE_HOST",
        "DBT_WAREHOUSE_USER",
        "DBT_WAREHOUSE_PASSWORD",
        "DBT_WAREHOUSE_DB",
    )
    missing = [key for key in required if not os.environ.get(key)]
    if missing:
        raise AirflowException(
            f"Missing warehouse env for sync watermark: {', '.join(missing)}"
        )
    return psycopg2.connect(
        host=os.environ["DBT_WAREHOUSE_HOST"],
        port=int(os.environ.get("DBT_WAREHOUSE_PORT", "5432")),
        user=os.environ["DBT_WAREHOUSE_USER"],
        password=os.environ["DBT_WAREHOUSE_PASSWORD"],
        dbname=os.environ["DBT_WAREHOUSE_DB"],
    )


def record_bronze_sync_watermark(
    *,
    bronze_schema: str,
    connection_id: str,
    pipeline_id: str | None,
    synced_at: Any,
    job_id: int,
    records_emitted: int | None = None,
    records_committed: int | None = None,
) -> None:
    """Upsert last successful Airbyte sync time for dbt source freshness."""
    if not bronze_schema.strip():
        raise AirflowException("bronze_schema is required to record sync watermark")

    with _warehouse_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_ENSURE_BRONZE_META_SQL)
            cur.execute(
                _UPSERT_WATERMARK_SQL,
                (
                    bronze_schema,
                    connection_id,
                    pipeline_id,
                    synced_at,
                    job_id,
                    records_emitted,
                    records_committed,
                ),
            )
        conn.commit()

    logger.info(
        "Bronze sync watermark recorded | schema=%s | job_id=%s | synced_at=%s | committed=%s",
        bronze_schema,
        job_id,
        synced_at,
        records_committed,
    )
