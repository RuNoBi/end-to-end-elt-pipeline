"""
Register one Airflow DAG per enabled file in airflow-platform/config/pipelines/*.yaml.

Add a pipeline: copy _template.yaml.example, set enabled: true, add dbt models + CKAN YAML.
Do not duplicate DAG definitions in elt_main_pipeline.py (kept only as a pointer).
"""

from __future__ import annotations

import logging

from common.elt_dag_builder import build_elt_dag
from common.pipeline_config import load_all_pipeline_configs

logger = logging.getLogger(__name__)

_pipeline_configs = load_all_pipeline_configs()
if not _pipeline_configs:
    logger.warning(
        "No enabled ELT pipelines found. Add YAML under config/pipelines/ "
        "and mount ./config → /opt/airflow/pipeline_config (see docker-compose.yml)."
    )

for _pipeline_cfg in _pipeline_configs:
    _dag = build_elt_dag(_pipeline_cfg)
    globals()[_pipeline_cfg["dag_id"]] = _dag
    logger.debug("Registered ELT DAG %s", _pipeline_cfg["dag_id"])
