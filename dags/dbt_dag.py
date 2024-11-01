import duckdb
import glob
import os

from pendulum import datetime
from airflow.operators.bash import BashOperator
from airflow.decorators import dag, task
from new_posterior_data_gen import create_new_csv
from time import time



@dag(
    dag_id="Create_ingest_run_dbt_dag",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    default_args={"owner": "Maksym Ionutsa", "depends_on_past": False, "retries": 0},
    default_view="graph",
)
def my_dag():
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
        # Assuming create_new_csv is defined elsewhere
        create_new_csv(base_folder, base_file_name, field_names)

    @task
    def transform_to_parquet():
        csv_dir = os.path.expanduser("/usr/local/airflow/post")
        csv_files = [file_name for file_name in os.listdir(csv_dir) if file_name.endswith(".csv")]
        batch_size = 1

        for i in range(0, len(csv_files), batch_size):
            batch_files = csv_files[i:i + batch_size]  

            for file_name in batch_files:
                f_path = os.path.join(csv_dir, file_name)

            
                base_name = os.path.splitext(file_name)[0]  
                parquet_dir = os.path.join(csv_dir, base_name)
                os.makedirs(parquet_dir, exist_ok=True) 
                par_path = os.path.join(parquet_dir, f"{base_name}.parquet")

                duckdb_cmd = f"COPY (SELECT * FROM read_csv_auto('{f_path}')) TO '{par_path}' (FORMAT PARQUET);"
                
                try:
                    duckdb.execute(duckdb_cmd)
                    print(f"Converted {f_path} to {par_path}")
                except Exception as e:
                    print(f"Error processing {f_path}: {e}")

            time.sleep(5) 
    @task
    def ingest():
        conn = duckdb.connect(database="/usr/local/airflow/ecom.db", read_only=False)
        csv_files = glob.glob("/usr/local/airflow/post/ecommerce_data_*.csv")

        for csv_file in csv_files:
            table_name = "ecomm_bronze_0"
            conn.execute(
                f"insert into {table_name} SELECT * FROM read_csv_auto('{csv_file}')"
            )
            print(f"The data from '{csv_file}' is inserted into '{table_name}'")

    @task
    def run_dbt():
        BashOperator(
            task_id="run_dbt",
            bash_command="dbt run",
            dag=my_dag,
        )

    @task
    def test_dbt():
        BashOperator(
            task_id="test_dbt",
            bash_command="dbt test",
            dag=my_dag,
        )

    create_new_csv_task = create_new_csv_task()
    ingest_task = ingest()
    run_dbt_task = run_dbt()
    test_dbt_task = test_dbt()

    create_new_csv_task >> ingest_task >> run_dbt_task >> test_dbt_task


dag = my_dag()
