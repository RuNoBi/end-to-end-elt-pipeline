"""Build dbt bash commands from pipeline config."""

from __future__ import annotations

from typing import Any

DBT_PROJECT_DIR = "/opt/dbt"
DBT_PROFILES_DIR = "/opt/dbt"
DBT_BIN = "/home/airflow/.dbt-venv/bin/dbt"

_PREAMBLE = f"""
set -euo pipefail
cd {DBT_PROJECT_DIR}
export DBT_PROFILES_DIR={DBT_PROFILES_DIR}
{DBT_BIN} deps --profiles-dir {DBT_PROFILES_DIR}
"""


def _select_arg(selector: str | None) -> str:
    if selector and selector.strip():
        return f' --select {selector.strip()}'
    return ""


def build_dbt_commands(dbt_cfg: dict[str, Any]) -> dict[str, str]:
    """Return bash command strings keyed by task role."""
    freshness = dbt_cfg.get("freshness_select")
    silver_run = dbt_cfg.get("silver_run_select", "staging+")
    silver_test = dbt_cfg.get("silver_test_select", "staging")
    gold_run = dbt_cfg.get("gold_run_select", "marts+")
    gold_test = dbt_cfg.get("gold_test_select", "marts+")

    cmds = {
        "freshness": _PREAMBLE
        + f"{DBT_BIN} source freshness --profiles-dir {DBT_PROFILES_DIR}"
        + _select_arg(freshness),
        "silver_run": _PREAMBLE
        + f"{DBT_BIN} run --profiles-dir {DBT_PROFILES_DIR}"
        + _select_arg(silver_run),
        "silver_test": _PREAMBLE
        + f"{DBT_BIN} test --profiles-dir {DBT_PROFILES_DIR}"
        + _select_arg(silver_test),
        "gold_run": _PREAMBLE
        + f"{DBT_BIN} run --profiles-dir {DBT_PROFILES_DIR}" + _select_arg(gold_run),
        "gold_test": _PREAMBLE
        + f"{DBT_BIN} test --profiles-dir {DBT_PROFILES_DIR}" + _select_arg(gold_test),
    }

    if dbt_cfg.get("run_singular_tests"):
        cmds["silver_test"] += (
            f"\n{DBT_BIN} test --profiles-dir {DBT_PROFILES_DIR} --select test_type:singular"
        )

    snapshots = dbt_cfg.get("snapshots") or []
    if snapshots:
        snap_select = " ".join(snapshots)
        cmds["snapshot"] = (
            _PREAMBLE
            + f"{DBT_BIN} snapshot --profiles-dir {DBT_PROFILES_DIR}"
            + _select_arg(snap_select)
        )

    return cmds
