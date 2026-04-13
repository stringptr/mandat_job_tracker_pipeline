from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

DEFAULT_ARGS = {
    "owner": "student_researcher",
    "start_date": datetime(2026, 4, 1),
}


with DAG(
    "silver_to_gold_processing",
    default_args=DEFAULT_ARGS,
    catchup=False,
) as dag:
    t1 = SQLExecuteQueryOperator(
        task_id="aggregate_to_gold",
        conn_id="lakehouse-silver",
        sql="sql/agregate_by_location.sql",
    )

    t1
