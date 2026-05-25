"""
Airflow failure alerting — email on task failure (after retries exhausted).

Configure via airflow-platform/.env:
  AIRFLOW_ALERT_EMAILS, AIRFLOW_SMTP_* , AIRFLOW_WEBSERVER_BASE_URL

See docs/AIRFLOW_ALERTING.md for Gmail / university SMTP setup.
"""

from __future__ import annotations

import html
import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

LAYER_HINTS: dict[str, str] = {
    "extraction": "Bronze ingest (Airbyte) — check connection, proxy, job API.",
    "dbt_run_silver": "Silver build — inspect dbt run log; often upstream Bronze/schema.",
    "dbt_test_silver": "Silver data-quality gate — search Log for 'Failure in test'.",
    "dbt_run_gold": "Gold build — inspect dbt run log and Silver inputs.",
    "dbt_test_gold": "Gold tests — relationships to dim_* or mart constraints.",
    "log_pipeline_status": "Monitoring task — usually secondary; check upstream failure first.",
}


def get_alert_recipients() -> list[str]:
    raw = os.environ.get("AIRFLOW_ALERT_EMAILS", "")
    return [addr.strip() for addr in raw.split(",") if addr.strip()]


def _task_layer_hint(task_id: str) -> str:
    for key, hint in LAYER_HINTS.items():
        if key in task_id:
            return hint
    return "Open task Log in Airflow UI for stack trace and dbt/Airbyte details."


def _build_log_url(context: dict[str, Any]) -> str:
    ti = context["task_instance"]
    base = os.environ.get("AIRFLOW_WEBSERVER_BASE_URL", "http://localhost:8080").rstrip("/")
    # Airflow 2 UI uses run_id; execution_date link still works on many versions
    run_id = getattr(ti, "run_id", None) or context.get("run_id", "")
    if run_id:
        return (
            f"{base}/dags/{ti.dag_id}/grid"
            f"?dag_run_id={html.escape(run_id, quote=True)}"
            f"&task_id={html.escape(ti.task_id, quote=True)}"
            f"&tab=logs"
        )
    return f"{base}/dags/{ti.dag_id}/grid"


def elt_task_failure_email(context: dict[str, Any]) -> None:
    """
    on_failure_callback: send one email per permanently failed task.
    Not invoked while Airflow is retrying (UP_FOR_RETRY).
    """
    recipients = get_alert_recipients()
    if not recipients:
        logger.warning(
            "AIRFLOW_ALERT_EMAILS is empty — skip failure email for %s",
            context["task_instance"].task_id,
        )
        return

    ti = context["task_instance"]
    dag = context["dag"]
    exception = context.get("exception")
    try_number = getattr(ti, "try_number", None)
    max_tries = getattr(ti, "max_tries", None)
    log_url = _build_log_url(context)
    hint = _task_layer_hint(ti.task_id)

    subject = (
        f"[ELT ALERT][FAILED] {dag.dag_id} · {ti.task_id} "
        f"· try {try_number}/{max_tries}"
    )

    body_html = f"""
    <html><body style="font-family: system-ui, sans-serif; line-height: 1.5;">
      <h2 style="color: #b42318;">Pipeline task failed</h2>
      <table cellpadding="6" style="border-collapse: collapse;">
        <tr><td><b>DAG</b></td><td>{html.escape(dag.dag_id)}</td></tr>
        <tr><td><b>Task</b></td><td>{html.escape(ti.task_id)}</td></tr>
        <tr><td><b>Run ID</b></td><td>{html.escape(str(ti.run_id))}</td></tr>
        <tr><td><b>Logical date</b></td><td>{html.escape(str(context.get("logical_date", "")))}</td></tr>
        <tr><td><b>Try</b></td><td>{try_number} / {max_tries}</td></tr>
      </table>
      <p><b>Triage hint:</b> {html.escape(hint)}</p>
      {"<pre style='background:#f4f4f5;padding:12px;'>" + html.escape(str(exception)[:2000]) + "</pre>" if exception else ""}
      <p><a href="{html.escape(log_url)}">Open task logs in Airflow</a></p>
      <hr/>
      <p style="color:#666;font-size:12px;">
        PoC ELT pipeline · email sent after retries exhausted ·
        do not reply to this message.
      </p>
    </body></html>
    """

    try:
        from airflow.utils.email import send_email

        send_email(
            to=recipients,
            subject=subject,
            html_content=body_html,
        )
        logger.info(
            "Failure alert sent to %s for %s.%s",
            recipients,
            dag.dag_id,
            ti.task_id,
        )
    except Exception as exc:
        logger.exception(
            "Could not send failure email for %s.%s: %s",
            dag.dag_id,
            ti.task_id,
            exc,
        )
