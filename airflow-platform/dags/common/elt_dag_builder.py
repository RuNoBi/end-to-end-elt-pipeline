"""Build one ELT DAG from a pipeline YAML config (factory pattern)."""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from common.airbyte_sync import make_airbyte_sync_callable
from common.alerting import elt_task_failure_email
from common.ckan_publish import make_ckan_publish_callable
from common.dbt_commands import build_dbt_commands

logger = logging.getLogger(__name__)

DEFAULT_ARGS_BASE = {
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": elt_task_failure_email,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "execution_timeout": timedelta(hours=2),
}


def _log_pipeline_run_status(dag_id: str, **context: Any) -> None:
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
        dag_id,
        dag_run.run_id,
        run_state,
        context["logical_date"],
    )
    logger.info("Tasks succeeded (%s): %s", len(success), success)
    if failed:
        logger.error("Tasks failed (%s): %s", len(failed), failed)
    if skipped:
        logger.warning("Tasks upstream_failed (%s): %s", len(skipped), skipped)

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
        logger.info("Pipeline %s completed successfully.", dag_id)


def build_elt_dag(cfg: dict[str, Any]) -> DAG:
    """Create Airbyte → dbt → CKAN DAG from one pipelines/*.yaml entry."""
    dag_id = cfg["dag_id"]
    pipeline_id = cfg.get("pipeline_id", dag_id)
    timezone = cfg.get("timezone", "Asia/Bangkok")
    start_raw = cfg.get("start_date", "2026-01-01")

    default_args = {
        **DEFAULT_ARGS_BASE,
        "owner": cfg.get("owner", "data-engineering"),
    }

    dbt_cmds = build_dbt_commands(cfg.get("dbt") or {})
    airbyte_cfg = cfg.get("airbyte") or {}
    ckan_cfg = cfg.get("ckan") or {}
    airbyte_callable = make_airbyte_sync_callable(airbyte_cfg)

    dag = DAG(
        dag_id=dag_id,
        description=cfg.get(
            "description",
            f"ELT pipeline {pipeline_id}: Airbyte → dbt → CKAN",
        ),
        default_args=default_args,
        schedule=cfg.get("schedule"),
        start_date=pendulum.parse(str(start_raw), tz=timezone),
        catchup=cfg.get("catchup", False),
        tags=cfg.get("tags") or ["elt"],
        max_active_runs=cfg.get("max_active_runs", 1),
    )

    with dag:
        with TaskGroup(
            group_id="extraction",
            tooltip="Bronze: Airbyte EL (OSS API)",
        ) as extraction_group:
            PythonOperator(
                task_id="trigger_airbyte_sync",
                task_display_name="Airbyte: Sync source → warehouse (Bronze)",
                python_callable=airbyte_callable,
            )

        with TaskGroup(
            group_id="validation",
            tooltip="Bronze SLA: source freshness",
        ) as validation_group:
            BashOperator(
                task_id="dbt_source_freshness",
                task_display_name="dbt: source freshness (Bronze SLA)",
                bash_command=dbt_cmds["freshness"],
            )

        with TaskGroup(
            group_id="transformation",
            tooltip="Silver gate → snapshots → Gold",
        ) as transformation_group:
            dbt_run_silver = BashOperator(
                task_id="dbt_run_silver",
                task_display_name="dbt: run Silver",
                bash_command=dbt_cmds["silver_run"],
            )

            silver_chain_tail = dbt_run_silver
            if "snapshot" in dbt_cmds:
                dbt_snapshot = BashOperator(
                    task_id="dbt_snapshot",
                    task_display_name="dbt: snapshots (SCD2)",
                    bash_command=dbt_cmds["snapshot"],
                )
                silver_chain_tail >> dbt_snapshot
                silver_chain_tail = dbt_snapshot

            dbt_test_silver = BashOperator(
                task_id="dbt_test_silver",
                task_display_name="dbt: test Silver (data quality gate)",
                bash_command=dbt_cmds["silver_test"],
            )
            dbt_run_gold = BashOperator(
                task_id="dbt_run_gold",
                task_display_name="dbt: run Gold",
                bash_command=dbt_cmds["gold_run"],
            )
            dbt_test_gold = BashOperator(
                task_id="dbt_test_gold",
                task_display_name="dbt: test Gold",
                bash_command=dbt_cmds["gold_test"],
            )

            silver_chain_tail >> dbt_test_silver >> dbt_run_gold >> dbt_test_gold

        with TaskGroup(
            group_id="monitoring",
            tooltip="Observability & pipeline summary",
        ) as monitoring_group:
            PythonOperator(
                task_id="log_pipeline_status",
                task_display_name="Monitor: pipeline run status",
                python_callable=_log_pipeline_run_status,
                op_kwargs={"dag_id": dag_id},
                trigger_rule="all_done",
            )

        chain = extraction_group >> validation_group >> transformation_group

        if ckan_cfg.get("enabled"):
            pubs_file = ckan_cfg.get("publications_file")
            if not pubs_file:
                raise ValueError(
                    f"Pipeline {pipeline_id}: ckan.enabled requires publications_file"
                )
            with TaskGroup(
                group_id="publication",
                tooltip="Publish Gold marts to CKAN Datastore",
            ) as publication_group:
                PythonOperator(
                    task_id="publish_gold_to_ckan",
                    task_display_name="CKAN: publish Gold datamarts",
                    python_callable=make_ckan_publish_callable(pubs_file),
                    execution_timeout=timedelta(hours=1),
                )
            chain >> publication_group >> monitoring_group
        else:
            chain >> monitoring_group

    return dag
