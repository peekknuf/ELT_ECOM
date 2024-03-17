from pendulum import datetime
from airflow.operators.bash import BashOperator
import duckdb
import glob
import os
import csv
from dotenv import load_dotenv
from airflow.decorators import dag, task
from new_posterior_data_gen import create_new_csv


@dag(
    dag_id="WE_GO",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    default_args={"owner": "peek", "retries": 0},
    default_view="graph",
)
def we_go_all_the_way():

    @task
    def create_new_csv_task():
        base_folder = "/usr/local/airflow/post"
        base_file_name = "ecommerce_data_{}.csv"
        field_names = [
            "id",
            "timestamp",
            "product_name",
            "price",
            "quantity",
            "category",
            "customer_name",
            "email",
            "address",
            "country",
            "payment_method",
            "phone_number",
            "discount_applied",
            "shipping_method",
            "order_status",
            "customer_age",
        ]
        create_new_csv(base_folder, base_file_name, field_names)

    @task
    def ingest():
        conn = duckdb.connect(database="/home/ecom.db", read_only=False)
        csv_files: list[str] = glob.glob(
            pathname="/usr/local/airflow/post/commerce_data_*.csv"
        )

        for idx, csv_file in enumerate(iterable=csv_files):
            table_name: str = f"econ{idx}"
            conn.execute(
                f"CREATE TABLE IF NOT EXISTS {table_name} AS FROM read_csv('{csv_file}')"
            )
            print(
                f"Table '{table_name}' created with data from '{csv_file}'"
            )  # intentional variable misspelling to proc the telegram message.

    @task
    def run_dbt():
        BashOperator(
            task_id="run_dbt",
            bash_command="dbt run",
            dag=dag,
        )

    @task
    def test_dbt():
        BashOperator(
            task_id="test_dbt",
            bash_command="dbt test",
            dag=dag,
        )

    create_new_csv_task = create_new_csv_task()
    ingest = ingest()
    run_dbt = run_dbt()
    test_dbt = test_dbt()
    create_new_csv_task >> ingest >> run_dbt >> test_dbt


we_go_all_the_way()
