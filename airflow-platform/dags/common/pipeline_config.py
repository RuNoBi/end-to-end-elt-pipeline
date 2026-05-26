"""Load per-pipeline YAML config (one file = one Airflow DAG)."""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from airflow.exceptions import AirflowException

_DEFAULT_CONFIG_ROOT = Path(__file__).resolve().parents[2] / "config"


def config_root() -> Path:
    raw = os.environ.get("ELT_PIPELINE_CONFIG_DIR", "").strip()
    if raw:
        return Path(raw)
    container = Path("/opt/airflow/pipeline_config")
    if container.is_dir():
        return container
    return _DEFAULT_CONFIG_ROOT


def pipelines_dir() -> Path:
    return config_root() / "pipelines"


def ckan_config_dir() -> Path:
    return config_root() / "ckan"


@lru_cache(maxsize=32)
def load_pipeline_config(pipeline_id: str) -> dict[str, Any]:
    path = pipelines_dir() / f"{pipeline_id}.yaml"
    if not path.is_file():
        raise AirflowException(f"Pipeline config not found: {path}")
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if data.get("pipeline_id") != pipeline_id:
        raise AirflowException(
            f"pipeline_id mismatch in {path.name}: "
            f"expected {pipeline_id!r}, got {data.get('pipeline_id')!r}"
        )
    return data


def load_all_pipeline_configs() -> list[dict[str, Any]]:
    configs: list[dict[str, Any]] = []
    for path in sorted(pipelines_dir().glob("*.yaml")):
        if path.name.startswith("_"):
            continue
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if not data.get("enabled", True):
            continue
        if not data.get("dag_id"):
            raise AirflowException(f"Missing dag_id in {path}")
        configs.append(data)
    return configs


def load_ckan_publications(filename: str) -> list[dict[str, str]]:
    path = ckan_config_dir() / filename
    if not path.is_file():
        raise AirflowException(f"CKAN publications config not found: {path}")
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    pubs = data.get("publications") or []
    if not isinstance(pubs, list):
        raise AirflowException(f"Invalid publications list in {path}")
    return pubs
