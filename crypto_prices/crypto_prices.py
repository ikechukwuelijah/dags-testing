from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import requests
import psycopg2
from psycopg2 import sql

# Default arguments for the DAG
default_args = {
    'owner': 'Ik',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'crypto_price_dag',
    default_args=default_args,
    description='A DAG to fetch cryptocurrency prices and insert them into a PostgreSQL database',
    schedule_interval=timedelta(hours=1),  # Run every hour
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Task to fetch cryptocurrency prices from CoinGecko API
    def fetch_crypto_prices():
        """
        Fetches cryptocurrency prices from CoinGecko API.
        Returns a list of dictionaries containing price data.
        """
        url = "https://api.coingecko.com/api/v3/simple/price"
        parameters = {
            'ids': 'bitcoin,ethereum,binancecoin',
            'vs_currencies': 'usd',
            'include_24hr_change': 'true'
        }

        try:
            response = requests.get(url, params=parameters)
            response.raise_for_status()  # Raise an exception for HTTP errors
            data = response.json()

            return [
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
            ]
        except requests.RequestException as e:
            raise ValueError(f"Error fetching data from CoinGecko API: {e}")

# Task to insert data into PostgreSQL using PostgresHook
    def insert_data_into_db(crypto_data):
        """
        Inserts cryptocurrency price data into a PostgreSQL database using PostgresHook.
        """
        if not crypto_data:
            raise ValueError("No data to insert.")

        try:
            # Use PostgresHook to get the connection
            hook = PostgresHook(postgres_conn_id='postgres_dwh')  # Replace with your connection ID
            conn = hook.get_conn()
            cursor = conn.cursor()

            # Define the SQL query
            insert_query = """
                INSERT INTO crypto_prices (symbol, price_usd, change_24h)
                VALUES (%s, %s, %s)
            """

            # Insert each record into the database
            for record in crypto_data:
                cursor.execute(insert_query, (
                    record['symbol'], record['price_usd'], record['change_24h']
                ))

            # Commit the transaction
            conn.commit()
            cursor.close()
            conn.close()

            print("Data inserted successfully.")
        except Exception as e:
            raise ValueError(f"Error inserting data into PostgreSQL: {e}")

    # Define the first task to fetch cryptocurrency prices
    fetch_prices_task = PythonOperator(
        task_id='fetch_crypto_prices',
        python_callable=fetch_crypto_prices,
        provide_context=True,  # Enable context passing between tasks
        dag=dag,
    )

    # Define the second task to insert data into the database
    insert_data_task = PythonOperator(
        task_id='insert_data_into_db',
        python_callable=insert_data_into_db,
        op_kwargs={'crypto_data': "{{ ti.xcom_pull(task_ids='fetch_crypto_prices') }}"},
        dag=dag,
    )

    # Set task dependencies
    fetch_prices_task >> insert_data_task