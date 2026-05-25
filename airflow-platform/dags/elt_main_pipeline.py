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

from common.alerting import elt_task_failure_email

logger = logging.getLogger(__name__)

DAG_ID = "elt_main_pipeline"
AIRBYTE_CONNECTION_ID = Variable.get(
    "airbyte_connection_id",
    default_var="b84b53a7-abfa-4c29-9f6b-c663dd0f4283",
)

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
{DBT_BIN} deps --profiles-dir {DBT_PROFILES_DIR} --quiet || true
"""

DBT_RUN_SILVER_CMD = _DBT_PREAMBLE + f"""
{DBT_BIN} run --profiles-dir {DBT_PROFILES_DIR} --select staging+ intermediate+
"""

DBT_TEST_SILVER_CMD = _DBT_PREAMBLE + f"""
{DBT_BIN} test --profiles-dir {DBT_PROFILES_DIR} --select staging intermediate
"""

DBT_RUN_GOLD_CMD = _DBT_PREAMBLE + f"""
{DBT_BIN} run --profiles-dir {DBT_PROFILES_DIR} --select marts+
"""

DBT_TEST_GOLD_CMD = _DBT_PREAMBLE + f"""
{DBT_BIN} test --profiles-dir {DBT_PROFILES_DIR} --select marts+
"""


def run_airbyte_oss_sync(
    connection_id: str | None = None,
    api_base_url: str | None = None,
    poll_interval_seconds: int = 30,
    timeout_seconds: int = 7200,
    **context: Any,
) -> dict[str, Any]:
    """Trigger and wait for Airbyte OSS 0.63 sync via Config API."""
    conn_id = connection_id or Variable.get(
        "airbyte_connection_id",
        default_var="b84b53a7-abfa-4c29-9f6b-c663dd0f4283",
    )
    base = (api_base_url or Variable.get(
        "airbyte_api_base_url",
        default_var="http://airbyte-proxy:8000/api/v1",
    )).rstrip("/")

    logger.info(
        "Starting Airbyte OSS sync | connection_id=%s | api_base=%s",
        conn_id,
        base,
    )

    try:
        trigger = requests.post(
            f"{base}/connections/sync",
            json={"connectionId": conn_id},
            timeout=120,
        )
        trigger.raise_for_status()
    except requests.RequestException as exc:
        raise AirflowException(f"Failed to trigger Airbyte sync: {exc}") from exc

    payload = trigger.json()
    job = payload.get("job") or {}
    job_id = job.get("id")
    if job_id is None:
        raise AirflowException(f"Unexpected Airbyte sync response: {payload}")

    logger.info(
        "Airbyte job started | job_id=%s | initial_status=%s",
        job_id,
        job.get("status"),
    )

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
        logger.info("Airbyte job %s status=%s", job_id, status)

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

    if run_state == "success":
        airbyte_xcom = context["ti"].xcom_pull(
            task_ids="extraction.trigger_airbyte_sync"
        )
        logger.info(
            "Airbyte connection_id=%s | sync_result_present=%s",
            AIRBYTE_CONNECTION_ID,
            airbyte_xcom is not None,
        )
        logger.info("Silver and Gold layers built and tested successfully.")


with DAG(
    dag_id=DAG_ID,
    description="End-to-end ELT: Airbyte OSS sync -> dbt run -> dbt test",
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
                "connection_id": AIRBYTE_CONNECTION_ID,
                "poll_interval_seconds": 30,
                "timeout_seconds": 7200,
            },
            doc_md=(
                f"Triggers connection `{AIRBYTE_CONNECTION_ID}` via "
                "`POST /api/v1/connections/sync` on Airbyte OSS 0.63.x. "
                "May run 10–60+ minutes for ~1M rows."
            ),
        )

    with TaskGroup(
        group_id="transformation",
        tooltip="Silver gate → Gold (dbt run + test per layer)",
    ) as transformation_group:
        dbt_run_silver = BashOperator(
            task_id="dbt_run_silver",
            task_display_name="dbt: run Silver (staging → intermediate)",
            bash_command=DBT_RUN_SILVER_CMD,
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

        dbt_run_silver >> dbt_test_silver >> dbt_run_gold >> dbt_test_gold

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

    extraction_group >> transformation_group >> monitoring_group
