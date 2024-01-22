import duckdb
import glob
import requests
import os
from dotenv import load_dotenv


load_dotenv()
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

def send_telegram_message(message):
    url = f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage'
    params = {
        'chat_id': TELEGRAM_CHAT_ID,
        'text': message
    }
    requests.post(url, params=params)


try:
    conn = duckdb.connect(database='~/ecom.db', read_only=False)
    csv_files = glob.glob('seeds/ecommerce_data_*.csv')

    for idx, csv_file in enumerate(csv_files):
        '''take all the raw csvs and ingest into tables '''
        table_name = f"econ{idx}" 
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS FROM read_csv_auto('{csv_file}')")
        print(f"Table '{table_123name}' created with data from '{csv_file}'") #intentional variable misspelling to proc the telegram message.

except Exception as e:
    error_message = f"The ingestion process has failed. \nAn error occurred: {str(e)}"
    print(error_message)
    
    send_telegram_message(error_message)
