"""Gold transform: dbt star-schema marts + movie_analytics wide table.

Runs after silver (Airflow Dataset `warehouse://silver`). dbt tests must pass.
Query gold in Metabase (http://localhost:3000).
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

SILVER_READY = Dataset("warehouse://silver")
GOLD_READY = Dataset("warehouse://gold")

DBT = (
    "dbt {cmd} "
    "--project-dir /opt/airflow/dbt "
    "--profiles-dir /opt/airflow/dbt "
    "--target-path /tmp/dbt-target "
    "--log-path /tmp/dbt-logs "
    "{select}"
)


@dag(
    dag_id="dag_tmdb_gold",
    description="dbt run+test gold dims/facts and movie_analytics",
    start_date=datetime(2024, 1, 1),
    schedule=[SILVER_READY],
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "tmdb-demo",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["tmdb", "gold", "dbt", "medallion"],
    doc_md=__doc__,
)
def dag_tmdb_gold():
    run = BashOperator(
        task_id="dbt_run_gold",
        bash_command=DBT.format(cmd="run", select="--select tag:gold"),
    )
    test = BashOperator(
        task_id="dbt_test_gold",
        bash_command=DBT.format(cmd="test", select="--select tag:gold"),
    )

    @task(outlets=[GOLD_READY])
    def mark_gold_ready() -> str:
        return "ok"

    run >> test >> mark_gold_ready()


dag_tmdb_gold()
