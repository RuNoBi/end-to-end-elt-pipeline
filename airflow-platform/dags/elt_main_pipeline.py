"""
ELT main pipeline: Airbyte OSS 0.63 sync -> dbt -> monitoring.

Uses the Airbyte Config API (/api/v1/connections/sync) because the bundled
AirbyteTriggerSyncOperator targets the newer Cloud /jobs API and fails on OSS.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from typing import Any

import requests
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from common.airbyte_validate import validate_airbyte_connection
from common.alerting import elt_task_failure_email
from common.ckan_publish import publish_gold_to_ckan

logger = logging.getLogger(__name__)

DAG_ID = "elt_main_pipeline"


def _require_airbyte_connection_id() -> str:
    conn_id = Variable.get("airbyte_connection_id", default_var="").strip()
    if not conn_id:
        raise AirflowException(
            "airbyte_connection_id is not set. Add AIRFLOW_VAR_AIRBYTE_CONNECTION_ID "
            "to airflow-platform/.env (from Airbyte UI → Connections)."
        )
    return conn_id


def _airbyte_api_base() -> str:
    return Variable.get(
        "airbyte_api_base_url",
        default_var="http://airbyte-proxy:8000/api/v1",
    ).rstrip("/")

DBT_PROJECT_DIR = "/opt/dbt"
DBT_PROFILES_DIR = "/opt/dbt"
DBT_BIN = "/home/airflow/.dbt-venv/bin/dbt"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": elt_task_failure_email,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "execution_timeout": timedelta(hours=2),
}

_DBT_PREAMBLE = f"""
set -euo pipefail
cd {DBT_PROJECT_DIR}
export DBT_PROFILES_DIR={DBT_PROFILES_DIR}
{DBT_BIN} deps --profiles-dir {DBT_PROFILES_DIR}
"""

DBT_SOURCE_FRESHNESS_CMD = _DBT_PREAMBLE + f"""
{DBT_BIN} source freshness --profiles-dir {DBT_PROFILES_DIR}
"""

DBT_RUN_SILVER_CMD = _DBT_PREAMBLE + f"""
{DBT_BIN} run --profiles-dir {DBT_PROFILES_DIR} --select staging+ intermediate+
"""

DBT_SNAPSHOT_CMD = _DBT_PREAMBLE + f"""
{DBT_BIN} snapshot --profiles-dir {DBT_PROFILES_DIR} --select snap_stg_customers
"""

DBT_TEST_SILVER_CMD = _DBT_PREAMBLE + f"""
{DBT_BIN} test --profiles-dir {DBT_PROFILES_DIR} --select staging intermediate source:src_local_postgres
{DBT_BIN} test --profiles-dir {DBT_PROFILES_DIR} --select test_type:singular
"""

DBT_RUN_GOLD_CMD = _DBT_PREAMBLE + f"""
{DBT_BIN} run --profiles-dir {DBT_PROFILES_DIR} --select marts+
"""

DBT_TEST_GOLD_CMD = _DBT_PREAMBLE + f"""
{DBT_BIN} test --profiles-dir {DBT_PROFILES_DIR} --select marts+
"""

_ACTIVE_AIRBYTE_SYNC_STATUSES = frozenset({"pending", "running", "incomplete"})


def _job_progress_stats(job_body: dict[str, Any]) -> dict[str, Any]:
    """Best-effort stats; jobs/get often omits aggregatedStats while still running."""
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
    """Return in-flight sync job id for this connection, if any."""
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
    """
    Start a new sync, or attach to one already running (HTTP 409 Conflict).
    """
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


def run_airbyte_oss_sync(
    connection_id: str | None = None,
    api_base_url: str | None = None,
    poll_interval_seconds: int = 30,
    timeout_seconds: int = 7200,
    **context: Any,
) -> dict[str, Any]:
    """Trigger and wait for Airbyte OSS 0.63 sync via Config API."""
    conn_id = connection_id or _require_airbyte_connection_id()
    base = (api_base_url or _airbyte_api_base()).rstrip("/")

    preflight = validate_airbyte_connection(base, conn_id)
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
    result = _wait_for_airbyte_job(base, job_id, poll_interval_seconds, timeout_seconds)
    job = result.get("job") or {}
    stats = _job_progress_stats(job)
    return {
        "preflight": preflight,
        "jobId": job_id,
        "status": job.get("status"),
        "recordsEmitted": stats.get("recordsEmitted"),
        "recordsCommitted": stats.get("recordsCommitted"),
    }


def log_pipeline_run_status(**context) -> None:
    """Run-level summary for Grid triage (runs even when upstream tasks fail)."""
    dag_run = context["dag_run"]
    task_states: dict[str, str | None] = {}

    for task_instance in dag_run.get_task_instances():
        task_states[task_instance.task_id] = task_instance.state

    failed = sorted(tid for tid, state in task_states.items() if state == "failed")
    skipped = sorted(
        tid for tid, state in task_states.items() if state == "upstream_failed"
    )
    success = sorted(tid for tid, state in task_states.items() if state == "success")

    run_state = dag_run.state
    logger.info(
        "PIPELINE RUN SUMMARY | dag=%s | run_id=%s | state=%s | logical_date=%s",
        DAG_ID,
        dag_run.run_id,
        run_state,
        context["logical_date"],
    )
    logger.info("Tasks succeeded (%s): %s", len(success), success)
    if failed:
        logger.error("Tasks failed (%s): %s", len(failed), failed)
    if skipped:
        logger.warning("Tasks upstream_failed (%s): %s", len(skipped), skipped)

    if any("dbt_source_freshness" in tid for tid in failed):
        logger.error(
            "HINT | Bronze data is stale — check Airbyte sync schedule and "
            "transformation.dbt_source_freshness log."
        )
    if any("dbt_test_silver" in tid for tid in failed):
        logger.error(
            "HINT | Silver data-quality gate failed — open Log on "
            "transformation.dbt_test_silver and search for 'FAIL' or 'relationships'."
        )
    if any("dbt_test_gold" in tid for tid in failed):
        logger.error(
            "HINT | Gold tests failed — open Log on transformation.dbt_test_gold."
        )
    if any("trigger_airbyte_sync" in tid for tid in failed):
        logger.error(
            "HINT | Extraction failed — check Airbyte UI jobs and "
            "scheduler Log for extraction.trigger_airbyte_sync."
        )
    if any("publish_gold_to_ckan" in tid for tid in failed):
        logger.error(
            "HINT | CKAN publish failed — check CKAN_URL/token and "
            "publication.publish_gold_to_ckan log."
        )

    if run_state == "success":
        airbyte_xcom = context["ti"].xcom_pull(
            task_ids="extraction.trigger_airbyte_sync"
        )
        if isinstance(airbyte_xcom, dict):
            logger.info(
                "Airbyte job_id=%s | emitted=%s | committed=%s",
                airbyte_xcom.get("jobId"),
                airbyte_xcom.get("recordsEmitted"),
                airbyte_xcom.get("recordsCommitted"),
            )
        logger.info("Bronze → Gold built, tested, and published to CKAN catalog.")


with DAG(
    dag_id=DAG_ID,
    description="End-to-end ELT: Airbyte → dbt Gold → CKAN catalog → monitor",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 7 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["elt", "airbyte", "dbt", "medallion", "production-poc"],
    max_active_runs=1,
) as dag:

    with TaskGroup(group_id="extraction", tooltip="Bronze: Airbyte EL (OSS API)") as extraction_group:
        trigger_airbyte_sync = PythonOperator(
            task_id="trigger_airbyte_sync",
            task_display_name="Airbyte: Sync source → warehouse (Bronze)",
            python_callable=run_airbyte_oss_sync,
            op_kwargs={
                "poll_interval_seconds": 30,
                "timeout_seconds": 7200,
            },
            doc_md=(
                "Validates incremental append_dedup streams, then triggers "
                "`POST /api/v1/connections/sync` on Airbyte OSS 0.63.x."
            ),
        )

    with TaskGroup(group_id="validation", tooltip="Bronze SLA: source freshness") as validation_group:
        dbt_source_freshness = BashOperator(
            task_id="dbt_source_freshness",
            task_display_name="dbt: source freshness (Bronze SLA)",
            bash_command=DBT_SOURCE_FRESHNESS_CMD,
        )

    with TaskGroup(
        group_id="transformation",
        tooltip="Silver gate → snapshots → Gold",
    ) as transformation_group:
        dbt_run_silver = BashOperator(
            task_id="dbt_run_silver",
            task_display_name="dbt: run Silver (staging → intermediate)",
            bash_command=DBT_RUN_SILVER_CMD,
        )

        dbt_snapshot_customers = BashOperator(
            task_id="dbt_snapshot_customers",
            task_display_name="dbt: snapshot customer history (SCD2)",
            bash_command=DBT_SNAPSHOT_CMD,
        )

        dbt_test_silver = BashOperator(
            task_id="dbt_test_silver",
            task_display_name="dbt: test Silver (data quality gate)",
            bash_command=DBT_TEST_SILVER_CMD,
        )

        dbt_run_gold = BashOperator(
            task_id="dbt_run_gold",
            task_display_name="dbt: run Gold (marts)",
            bash_command=DBT_RUN_GOLD_CMD,
        )

        dbt_test_gold = BashOperator(
            task_id="dbt_test_gold",
            task_display_name="dbt: test Gold",
            bash_command=DBT_TEST_GOLD_CMD,
        )

        (
            dbt_run_silver
            >> dbt_snapshot_customers
            >> dbt_test_silver
            >> dbt_run_gold
            >> dbt_test_gold
        )

    with TaskGroup(
        group_id="monitoring",
        tooltip="Observability & pipeline summary",
    ) as monitoring_group:
        log_pipeline_status_task = PythonOperator(
            task_id="log_pipeline_status",
            task_display_name="Monitor: pipeline run status",
            python_callable=log_pipeline_run_status,
            trigger_rule="all_done",
        )

    with TaskGroup(
        group_id="publication",
        tooltip="Publish Gold marts to CKAN Datastore (open data catalog)",
    ) as publication_group:
        publish_gold_to_ckan_task = PythonOperator(
            task_id="publish_gold_to_ckan",
            task_display_name="CKAN: publish Gold datamarts",
            python_callable=publish_gold_to_ckan,
            execution_timeout=timedelta(hours=1),
            doc_md=(
                "Copies `gold.mart_sales_performance` and `gold.dim_customer` "
                "into CKAN Datastore (UBE Group Thailand catalog) at http://localhost:5001"
            ),
        )

    (
        extraction_group
        >> validation_group
        >> transformation_group
        >> publication_group
        >> monitoring_group
    )
