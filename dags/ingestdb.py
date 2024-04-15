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
    csv_files = glob.glob(pathname="dbt/data_pipeline/seeds/ecommerce_data_*.csv")

    for idx, csv_file in enumerate(iterable=csv_files):
        table_name: str = f"ecomm_bronze_{idx}"
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS {table_name} AS FROM read_csv_auto('{csv_file}')"
        )
        print(f"Table '{table_name}' created with data from '{csv_file}'")
except Exception as e:
    error_message = f"The ingestion process has failed. \nAn error occurred: {str(e)}"
    print(error_message)

    send_telegram_message(error_message)
