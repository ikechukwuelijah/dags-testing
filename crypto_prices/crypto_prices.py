from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests
import psycopg2
from psycopg2 import sql

# CoinGecko API URL
url = "https://api.coingecko.com/api/v3/simple/price"
parameters = {
    'ids': 'bitcoin,ethereum,binancecoin',
    'vs_currencies': 'usd',
    'include_24hr_change': 'true'
}

def fetch_crypto_prices(**context):
    """
    Fetches cryptocurrency prices from the CoinGecko API.
    """
    try:
        response = requests.get(url, params=parameters)
        response.raise_for_status()
        data = response.json()
        # Pass fetched data to the next task
        context['task_instance'].xcom_push(key='crypto_data', value=[
            {
                'symbol': 'BTC',
                'price_usd': data['bitcoin']['usd'],
                'change_24h': data['bitcoin']['usd_24h_change']
            },
            {
                'symbol': 'ETH',
                'price_usd': data['ethereum']['usd'],
                'change_24h': data['ethereum']['usd_24h_change']
            },
            {
                'symbol': 'BNB',
                'price_usd': data['binancecoin']['usd'],
                'change_24h': data['binancecoin']['usd_24h_change']
            }
        ])
    except requests.RequestException as e:
        print(f"Error fetching data from CoinGecko API: {e}")

def insert_data_into_db(**context):
    """
    Inserts cryptocurrency prices into the PostgreSQL database.
    """
    crypto_data = context['task_instance'].xcom_pull(key='crypto_data', task_ids='fetch_crypto_prices')
    try:
        connection = psycopg2.connect(
            dbname='dwh',  # Replace with your database name
            user='ikeengr',  # Replace with your PostgreSQL username
            password='DataEngineer247',  # Replace with your PostgreSQL password
            host='89.40.0.150',  # Replace with your PostgreSQL server address if not local
            port='5432'  # Default PostgreSQL port
        )
        cursor = connection.cursor()
        
        insert_query = sql.SQL("""
            INSERT INTO crypto_prices (symbol, price_usd, change_24h)
            VALUES (%s, %s, %s)
        """)

        for record in crypto_data:
            cursor.execute(insert_query, (record['symbol'], record['price_usd'], record['change_24h']))
        
        connection.commit()
        cursor.close()
        connection.close()
        print("Data inserted successfully.")
    except psycopg2.DatabaseError as e:
        print(f"Error inserting data into PostgreSQL: {e}")

# Define default arguments for the DAG
default_args = {
    'owner': 'Ik',
    'start_date': datetime(2025, 2, 23),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG('crypto_price_dag', default_args=default_args, schedule_interval='@hourly', catchup=False) as dag:
    
    # Task to fetch cryptocurrency prices
    fetch_crypto_prices_task = PythonOperator(
        task_id='fetch_crypto_prices',
        provide_context=True,
        python_callable=fetch_crypto_prices
    )

    # Task to insert cryptocurrency prices into the database
    insert_data_into_db_task = PythonOperator(
        task_id='insert_data_into_db',
        provide_context=True,
        python_callable=insert_data_into_db
    )

    # Define task dependencies
    fetch_crypto_prices_task >> insert_data_into_db_task
