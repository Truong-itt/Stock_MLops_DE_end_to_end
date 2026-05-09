from __future__ import annotations

import os
import shlex
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Optional

from airflow.operators.bash import BashOperator


DEFAULT_START_DATE = datetime(2026, 4, 15, tzinfo=timezone.utc)

STOCK_SYSTEM_ROOT = Path(
    os.getenv("STOCK_SYSTEM_ROOT", "/opt/project/Stock_system")
)
DATA_DIR = STOCK_SYSTEM_ROOT / "data"

COMMON_ENV = {
    "PYTHONUNBUFFERED": "1",
    "SYMBOL_REGISTRY_PATH": os.getenv(
        "SYMBOL_REGISTRY_PATH", "/app/config/symbol_registry.json"
    ),
    "TRANSFORMERS_CACHE": os.getenv(
        "TRANSFORMERS_CACHE", "/app/.cache/huggingface"
    ),
}

DAEMON_DEFAULT_ARGS = {
    "owner": "stock-data",
    "depends_on_past": False,
    "retries": 999999,
    "retry_delay": timedelta(seconds=15),
}

PERIODIC_DEFAULT_ARGS = {
    "owner": "stock-data",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def build_bash_command(script_name: str, args: str = "") -> str:
    data_dir = shlex.quote(str(DATA_DIR))
    script = shlex.quote(script_name)
    extra_args = args.strip()
    if extra_args:
        return f"set -euo pipefail; cd {data_dir}; python -u {script} {extra_args}"
    return f"set -euo pipefail; cd {data_dir}; python -u {script}"


def make_data_task(
    task_id: str,
    script_name: str,
    args: str = "",
    extra_env: Optional[Dict[str, str]] = None,
) -> BashOperator:
    env = dict(COMMON_ENV)
    if extra_env:
        env.update(extra_env)

    return BashOperator(
        task_id=task_id,
        bash_command=build_bash_command(script_name=script_name, args=args),
        env=env,
        append_env=True,
    )
