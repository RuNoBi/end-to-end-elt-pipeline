"""Airbyte OSS 0.63 sync via Config API (shared across pipeline DAGs)."""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any

import requests
from airflow.exceptions import AirflowException
from airflow.models import Variable

from common.airbyte_validate import validate_airbyte_connection
from common.bronze_watermark import record_bronze_sync_watermark

logger = logging.getLogger(__name__)

_ACTIVE_AIRBYTE_SYNC_STATUSES = frozenset({"pending", "running", "incomplete"})


def _airbyte_api_base(variable_name: str = "airbyte_api_base_url") -> str:
    return Variable.get(
        variable_name,
        default_var="http://airbyte-proxy:8000/api/v1",
    ).rstrip("/")


def _require_connection_id(variable_name: str) -> str:
    conn_id = Variable.get(variable_name, default_var="").strip()
    if not conn_id:
        raise AirflowException(
            f"Airflow Variable {variable_name!r} is not set. "
            "Add AIRFLOW_VAR_<NAME> to airflow-platform/.env (UUID from Airbyte UI)."
        )
    return conn_id


def _job_progress_stats(job_body: dict[str, Any]) -> dict[str, Any]:
    stats = job_body.get("aggregatedStats") or {}
    if stats.get("recordsEmitted") is not None or stats.get("recordsCommitted") is not None:
        return stats

    stream_stats = job_body.get("streamAggregatedStats") or []
    if stream_stats:
        emitted = sum(int(s.get("recordsEmitted") or 0) for s in stream_stats)
        committed = sum(int(s.get("recordsCommitted") or 0) for s in stream_stats)
        return {"recordsEmitted": emitted, "recordsCommitted": committed}

    return stats


def _find_active_airbyte_sync_job_id(base: str, connection_id: str) -> int | None:
    response = requests.post(
        f"{base}/jobs/list",
        json={
            "configTypes": ["sync"],
            "configId": connection_id,
            "pagination": {"pageSize": 20},
        },
        timeout=60,
    )
    response.raise_for_status()
    for item in response.json().get("jobs") or []:
        job = item.get("job") or {}
        if (job.get("status") or "").lower() in _ACTIVE_AIRBYTE_SYNC_STATUSES:
            job_id = job.get("id")
            if job_id is not None:
                return int(job_id)
    return None


def _trigger_or_attach_airbyte_sync(base: str, connection_id: str) -> int:
    trigger = requests.post(
        f"{base}/connections/sync",
        json={"connectionId": connection_id},
        timeout=120,
    )

    if trigger.status_code == 409:
        job_id = _find_active_airbyte_sync_job_id(base, connection_id)
        if job_id is not None:
            logger.warning(
                "Airbyte sync already in progress (409) — waiting on existing job_id=%s",
                job_id,
            )
            return job_id
        raise AirflowException(
            "Airbyte returned 409 Conflict but no active sync job was found. "
            "Open Airbyte UI → Connections → check running/cancelled jobs."
        )

    try:
        trigger.raise_for_status()
    except requests.RequestException as exc:
        raise AirflowException(f"Failed to trigger Airbyte sync: {exc}") from exc

    job = trigger.json().get("job") or {}
    job_id = job.get("id")
    if job_id is None:
        raise AirflowException(f"Unexpected Airbyte sync response: {trigger.json()}")
    return int(job_id)


def _wait_for_airbyte_job(
    base: str,
    job_id: int,
    poll_interval_seconds: int,
    timeout_seconds: int,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            status_resp = requests.post(
                f"{base}/jobs/get",
                json={"id": job_id},
                timeout=60,
            )
            status_resp.raise_for_status()
        except requests.RequestException as exc:
            logger.warning("Poll failed for job %s, retrying: %s", job_id, exc)
            time.sleep(poll_interval_seconds)
            continue

        job_body = status_resp.json().get("job") or {}
        status = (job_body.get("status") or "").lower()
        stats = _job_progress_stats(job_body)
        logger.info(
            "Airbyte job %s status=%s | recordsEmitted=%s | recordsCommitted=%s",
            job_id,
            status,
            stats.get("recordsEmitted"),
            stats.get("recordsCommitted"),
        )

        if status == "succeeded":
            return status_resp.json()

        if status in {"failed", "cancelled"}:
            raise AirflowException(
                f"Airbyte job {job_id} ended with status={status}: {status_resp.json()}"
            )

        time.sleep(poll_interval_seconds)

    raise AirflowException(
        f"Timeout after {timeout_seconds}s waiting for Airbyte job {job_id}"
    )


def make_airbyte_sync_callable(airbyte_cfg: dict[str, Any]):
    """Factory: returns a PythonOperator callable bound to one pipeline's Airbyte config."""

    conn_var = airbyte_cfg.get("connection_id_variable", "airbyte_connection_id")
    api_var = airbyte_cfg.get("api_base_url_variable", "airbyte_api_base_url")
    poll_interval = int(airbyte_cfg.get("poll_interval_seconds", 30))
    timeout = int(airbyte_cfg.get("timeout_seconds", 7200))
    expected_streams = airbyte_cfg.get("expected_streams") or {}
    bronze_schema = (airbyte_cfg.get("bronze_schema") or "").strip()
    pipeline_id = (airbyte_cfg.get("pipeline_id") or "").strip() or None

    def run_airbyte_oss_sync(**context: Any) -> dict[str, Any]:
        conn_id = _require_connection_id(conn_var)
        base = _airbyte_api_base(api_var)

        preflight = validate_airbyte_connection(
            base,
            conn_id,
            expected_streams=expected_streams,
        )
        logger.info(
            "Starting Airbyte OSS sync | connection_id=%s | api_base=%s",
            conn_id,
            base,
        )

        try:
            job_id = _trigger_or_attach_airbyte_sync(base, conn_id)
        except requests.RequestException as exc:
            raise AirflowException(f"Failed to reach Airbyte API: {exc}") from exc

        logger.info("Airbyte sync job_id=%s — polling until terminal state", job_id)
        result = _wait_for_airbyte_job(base, job_id, poll_interval, timeout)
        job = result.get("job") or {}
        stats = _job_progress_stats(job)
        emitted = stats.get("recordsEmitted")
        committed = stats.get("recordsCommitted")
        payload = {
            "preflight": preflight,
            "jobId": job_id,
            "status": job.get("status"),
            "recordsEmitted": emitted,
            "recordsCommitted": committed,
        }

        if bronze_schema:
            synced_at = datetime.now(timezone.utc)
            record_bronze_sync_watermark(
                bronze_schema=bronze_schema,
                connection_id=conn_id,
                pipeline_id=pipeline_id,
                synced_at=synced_at,
                job_id=job_id,
                records_emitted=int(emitted) if emitted is not None else None,
                records_committed=int(committed) if committed is not None else None,
            )
            payload["bronzeWatermark"] = {
                "bronze_schema": bronze_schema,
                "synced_at": synced_at.isoformat(),
            }
        else:
            logger.warning(
                "airbyte.bronze_schema not set — skipping bronze_meta.sync_watermarks update"
            )

        return payload

    return run_airbyte_oss_sync
