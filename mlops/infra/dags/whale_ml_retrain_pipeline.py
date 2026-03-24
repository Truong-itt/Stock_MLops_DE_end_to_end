from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional
from urllib import error, request

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator


DAG_ID = "whale_ml_retrain_pipeline"
SERVICE_BASE_URL = os.getenv("WHALE_ML_SERVICE_URL", "http://whale-ml-service:8090").rstrip("/")


default_args = {
    "owner": "mlops",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

def _http_json(
    method: str,
    url: str,
    payload: Optional[Dict[str, Any]] = None,
    timeout: int = 45,
) -> Dict[str, Any]:
    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
    req = request.Request(
        url=url,
        method=method.upper(),
        data=data,
        headers={"Content-Type": "application/json"},
    )
    try:
        with request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8") or "{}"
            return json.loads(body)
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"{method} {url} failed with HTTP {exc.code}: {body}")
    except Exception as exc:
        raise RuntimeError(f"{method} {url} failed: {exc}")


def _read_optional_int(var_name: str) -> Optional[int]:
    raw = Variable.get(var_name, default_var=None)
    if raw is None or str(raw).strip() == "":
        return None
    return int(raw)


def check_whale_ml_health(**_: Any) -> Dict[str, Any]:
    response = _http_json("GET", f"{SERVICE_BASE_URL}/health")
    if response.get("status") != "ok":
        raise RuntimeError(f"Unexpected health response: {response}")
    return response


def trigger_whale_ml_train(**context: Any) -> Dict[str, Any]:
    dag_run = context.get("dag_run")
    run_conf = (dag_run.conf or {}) if dag_run else {}

    # Priority: dag_run.conf -> Airflow Variables -> service defaults.
    lookback_days = run_conf.get("lookback_days", _read_optional_int("whale_ml_lookback_days"))
    max_rows = run_conf.get("max_rows", _read_optional_int("whale_ml_max_rows"))
    horizon = run_conf.get("horizon", _read_optional_int("whale_ml_horizon"))
    timeout_seconds = run_conf.get(
        "timeout_seconds",
        _read_optional_int("whale_ml_train_timeout_seconds"),
    )

    train_payload: Dict[str, Any] = {}
    if lookback_days is not None:
        train_payload["lookback_days"] = int(lookback_days)
    if max_rows is not None:
        train_payload["max_rows"] = int(max_rows)
    if horizon is not None:
        train_payload["horizon"] = int(horizon)

    request_timeout = int(timeout_seconds) if timeout_seconds is not None else 900
    request_timeout = max(request_timeout, 60)
    response = _http_json(
        "POST",
        f"{SERVICE_BASE_URL}/train",
        payload=train_payload,
        timeout=request_timeout,
    )
    if response.get("status") != "ok":
        raise RuntimeError(f"Training response not ok: {response}")

    data = response.get("data", {})
    required_fields = ("mlflow_run_id", "model_name", "model_version", "model_uri")
    missing = [field for field in required_fields if not data.get(field)]
    if missing:
        raise RuntimeError(
            "Training completed but MLflow registry metadata is missing: "
            f"{', '.join(missing)}. Full response={response}"
        )
    return response


def verify_model_ready(**_: Any) -> Dict[str, Any]:
    response = _http_json("GET", f"{SERVICE_BASE_URL}/model/info")
    if response.get("status") != "ok":
        raise RuntimeError(f"Model info response not ok: {response}")
    model_data = response.get("data", {})
    if not model_data.get("ready"):
        raise RuntimeError(f"Model is not ready after train: {response}")
    if not model_data.get("trained_at"):
        raise RuntimeError(f"Model does not expose trained_at: {response}")
    if not model_data.get("mlflow_run_id"):
        raise RuntimeError(f"Model does not expose mlflow_run_id: {response}")
    if not model_data.get("model_version"):
        raise RuntimeError(f"Model does not expose model_version: {response}")
    return response


with DAG(
    dag_id=DAG_ID,
    description="Train/retrain whale ML model from ClickHouse anomalies and register in MLOps flow",
    default_args=default_args,
    start_date=datetime(2026, 3, 24, tzinfo=timezone.utc),
    schedule="0 */6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["mlops", "whale-ml", "retrain"],
    is_paused_upon_creation=False,
) as dag:
    check_health = PythonOperator(
        task_id="check_whale_ml_health",
        python_callable=check_whale_ml_health,
    )

    train_model = PythonOperator(
        task_id="trigger_whale_ml_train",
        python_callable=trigger_whale_ml_train,
    )

    verify_ready = PythonOperator(
        task_id="verify_model_ready",
        python_callable=verify_model_ready,
    )

    check_health >> train_model >> verify_ready
