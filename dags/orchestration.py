from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "peekknuf",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG("data_workflow_with_dbt", default_args=default_args, schedule="@daily")

generate_data = BashOperator(
    task_id="generate_data",
    bash_command="python ~/code/ecom/new_posterior_data_gen.py",
    dag=dag,
)

ingest_data = BashOperator(
    task_id="ingest_data",
    bash_command="python ~/code/ecom/ingestdb.py",
    dag=dag,
)

run_dbt = BashOperator(
    task_id="run_dbt",
    bash_command="dbt run",
    dag=dag,
)

generate_data >> ingest_data >> run_dbt
