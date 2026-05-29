"""Build dbt bash commands from pipeline config."""

from __future__ import annotations

from typing import Any

DBT_PROJECT_DIR = "/opt/dbt"
DBT_PROFILES_DIR = "/opt/dbt"
DBT_BIN = "/home/airflow/.dbt-venv/bin/dbt"
# Baked into the Airflow image at build time (not on the bind-mounted project).
DBT_PACKAGES_INSTALL_PATH = "/opt/dbt-vendor/dbt_packages"

# Per task run: isolated compile/run artifacts (safe for many concurrent DAGs).
_ENV = f"""
set -euo pipefail
cd {DBT_PROJECT_DIR}
export DBT_PROFILES_DIR={DBT_PROFILES_DIR}
export DBT_PACKAGES_INSTALL_PATH={DBT_PACKAGES_INSTALL_PATH}
export DBT_TARGET_PATH="/tmp/dbt-target/{{{{ dag.dag_id }}}}/{{{{ run_id | replace(':', '_') | replace('+', '_') }}}}/{{{{ task_instance.task_id }}}}"
mkdir -p "$DBT_TARGET_PATH"
if [ ! -f {DBT_PACKAGES_INSTALL_PATH}/dbt_utils/dbt_project.yml ]; then
  echo "ERROR: dbt packages missing at {DBT_PACKAGES_INSTALL_PATH} — rebuild Airflow image after packages.yml changes"
  exit 1
fi
"""


def _select_arg(selector: str | None) -> str:
    if selector and selector.strip():
        return f' --select {selector.strip()}'
    return ""


def _dbt_cmd(subcommand: str, selector: str | None = None) -> str:
    return (
        _ENV
        + f"{DBT_BIN} {subcommand} --project-dir {DBT_PROJECT_DIR}"
        + f" --profiles-dir {DBT_PROFILES_DIR}"
        + _select_arg(selector)
    )


def build_dbt_commands(dbt_cfg: dict[str, Any]) -> dict[str, str]:
    """Return bash command strings keyed by task role."""
    freshness = dbt_cfg.get("freshness_select")
    silver_run = dbt_cfg.get("silver_run_select", "tag:medallion_silver")
    silver_test = dbt_cfg.get("silver_test_select", "tag:medallion_silver")
    gold_run = dbt_cfg.get("gold_run_select", "tag:gold")
    gold_test = dbt_cfg.get("gold_test_select", "tag:gold")

    cmds: dict[str, str] = {
        "freshness": _dbt_cmd("source freshness", freshness),
        "silver_run": _dbt_cmd("run", silver_run),
        "silver_test": _dbt_cmd("test", silver_test),
        "gold_run": _dbt_cmd("run", gold_run),
        "gold_test": _dbt_cmd("test", gold_test),
    }

    if dbt_cfg.get("run_singular_tests"):
        cmds["silver_test"] += "\n" + _dbt_cmd("test", "test_type:singular")

    snapshots = dbt_cfg.get("snapshots") or []
    if snapshots:
        snap_select = " ".join(snapshots)
        cmds["snapshot"] = _dbt_cmd("snapshot", snap_select)

    return cmds
