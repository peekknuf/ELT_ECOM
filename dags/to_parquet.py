import os
import duckdb
import logging
import time

logging.basicConfig(filename='conversion.log', level=logging.INFO)
def transform_to_parquet():
    csv_dir = os.path.expanduser("/home/peek/code/astrocheck/dags/dbt/data_pipeline/seeds")
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