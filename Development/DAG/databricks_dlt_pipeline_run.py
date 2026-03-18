from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False
}

with DAG(
    dag_id="databricks_job_run",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["databricks", "etl"]
) as dag:

    run_job = DatabricksRunNowOperator(
        task_id="run_databricks_job",
        databricks_conn_id="databricks_default",
        job_id=1004884495583993
    )

    run_job