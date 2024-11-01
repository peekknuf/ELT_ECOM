import duckdb
import glob
import requests
import os
from dotenv import load_dotenv


load_dotenv()
TELEGRAM_BOT_TOKEN = os.getenv(key="TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv(key="TELEGRAM_CHAT_ID")


def send_telegram_message(message) -> None:
    url: str = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    params = {"chat_id": TELEGRAM_CHAT_ID, "text": message}
    requests.post(url=url, params=params)


try:
    conn = duckdb.connect(database="~/ecom.db", read_only=False)
    data_dirs = glob.glob("dbt/data_pipeline/seeds/ecommerce_data_*")

    for idx, data_dir in enumerate(data_dirs):
        parquet_files = glob.glob(os.path.join(data_dir, "*.parquet"))

        for parquet_idx, parquet_file in enumerate(parquet_files):
            table_name = f"ecomm_bronze_{idx}_{parquet_idx}"
            conn.execute(
                f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM parquet_scan('{parquet_file}')"
            )
            print(f"Table '{table_name}' created with data from '{parquet_file}'")

    conn.close()
except Exception as e:
    error_message = f"The ingestion process has failed. \nAn error occurred: {str(e)}"
    print(error_message)

    send_telegram_message(error_message)
