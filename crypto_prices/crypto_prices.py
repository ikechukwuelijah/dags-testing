from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from datetime import timedelta
import requests
import json  

# Default arguments for the DAG
default_args = {
    'owner': 'Ik',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Function to fetch cryptocurrency prices
def fetch_crypto_prices():
    url = "https://api.coingecko.com/api/v3/simple/price"
    parameters = {
        'ids': 'bitcoin,ethereum,binancecoin',
        'vs_currencies': 'usd',
        'include_24hr_change': 'true'
    }
    try:
        response = requests.get(url, params=parameters)
        response.raise_for_status()
        data = response.json()

        return [
            {'symbol': 'BTC', 'price_usd': data['bitcoin']['usd'], 'change_24h': data['bitcoin']['usd_24h_change']},
            {'symbol': 'ETH', 'price_usd': data['ethereum']['usd'], 'change_24h': data['ethereum']['usd_24h_change']},
            {'symbol': 'BNB', 'price_usd': data['binancecoin']['usd'], 'change_24h': data['binancecoin']['usd_24h_change']}
        ]
    except requests.RequestException as e:
        raise ValueError(f"Error fetching data from CoinGecko API: {e}")

# Function to insert data into PostgreSQL
def insert_into_postgres(**kwargs):
    ti = kwargs['ti']
    crypto_data = ti.xcom_pull(task_ids='fetch_crypto_prices')
    if not crypto_data:
        raise ValueError("No data received from fetch task")

    pg_hook = PostgresHook(postgres_conn_id='postgres_crypto')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    
    insert_query = """
    INSERT INTO crypto_prices (symbol, price_usd, change_24h, timestamp)
    VALUES (%s, %s, %s, NOW())
    """
    
    for crypto in crypto_data:
        cursor.execute(insert_query, (crypto['symbol'], crypto['price_usd'], crypto['change_24h']))
    
    connection.commit()
    cursor.close()
    connection.close()

# Define the DAG
with DAG(
    'crypto_price_dag',
    default_args=default_args,
    description='A DAG to fetch cryptocurrency prices and insert them into a PostgreSQL database',
    schedule_interval=timedelta(hours=1),
    start_date=days_ago(1),
    catchup=False,
) as dag:
    
    fetch_task = PythonOperator(
        task_id='fetch_crypto_prices',
        python_callable=fetch_crypto_prices,
        do_xcom_push=True
    )
    
    insert_task = PythonOperator(
        task_id='insert_into_postgres',
        python_callable=insert_into_postgres,
        provide_context=True
    )
    
    fetch_task >> insert_task
