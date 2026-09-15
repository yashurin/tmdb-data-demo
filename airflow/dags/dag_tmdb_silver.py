"""Silver transform: dbt models from warehouse.bronze.raw_payloads jsonb.

Runs after bronze (Airflow Dataset `warehouse://bronze`). dbt tests must pass
or the DAG fails. Emits `warehouse://silver` for gold.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

BRONZE_READY = Dataset("warehouse://bronze")
SILVER_READY = Dataset("warehouse://silver")

DBT = (
    "dbt {cmd} "
    "--project-dir /opt/airflow/dbt "
    "--profiles-dir /opt/airflow/dbt "
    "--target-path /tmp/dbt-target "
    "--log-path /tmp/dbt-logs "
    "{select}"
)


@dag(
    dag_id="dag_tmdb_silver",
    description="dbt run+test silver conformed tables from bronze jsonb",
    start_date=datetime(2024, 1, 1),
    schedule=[BRONZE_READY],
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "tmdb-demo",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["tmdb", "silver", "dbt", "medallion"],
    doc_md=__doc__,
)
def dag_tmdb_silver():
    run = BashOperator(
        task_id="dbt_run_silver",
        bash_command=DBT.format(cmd="run", select="--select tag:silver"),
    )
    test = BashOperator(
        task_id="dbt_test_silver",
        bash_command=DBT.format(cmd="test", select="--select tag:silver"),
    )

    @task(outlets=[SILVER_READY])
    def mark_silver_ready() -> str:
        return "ok"

    run >> test >> mark_silver_ready()


dag_tmdb_silver()
