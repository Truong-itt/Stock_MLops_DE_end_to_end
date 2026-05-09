from __future__ import annotations

from datetime import timedelta

from airflow import DAG

from stock_data_airflow_common import (
    DEFAULT_START_DATE,
    PERIODIC_DEFAULT_ARGS,
    make_data_task,
)


with DAG(
    dag_id="stock_data_news_worker",
    description="Run one news crawl cycle every 5 minutes",
    default_args=PERIODIC_DEFAULT_ARGS,
    start_date=DEFAULT_START_DATE,
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=5),
    tags=["stock-data", "news", "crawler"],
    is_paused_upon_creation=False,
) as dag:
    make_data_task(
        task_id="run_news_worker",
        script_name="news_worker.py",
        args="--once",
    )
