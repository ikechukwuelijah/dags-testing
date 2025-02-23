from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from datetime import timedelta
import requests
import pandas as pd
import json
from psycopg2.extras import execute_batch  # For batch execution

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
    'crypto_price_dag_bulk_insert',
    default_args=default_args,
    description='A DAG to fetch cryptocurrency prices, transform, and bulk insert into PostgreSQL',
    schedule_interval=timedelta(hours=1),  # Run every hour
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Task to fetch cryptocurrency prices from CoinGecko API
    def fetch_crypto_prices(**kwargs):
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

            crypto_data = [
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

            # Push data to XCom
            ti = kwargs['ti']
            ti.xcom_push(key='fetched_data', value=json.dumps(crypto_data))

        except requests.RequestException as e:
            raise ValueError(f"Error fetching data from CoinGecko API: {e}")

    # Task to transform the fetched data
    def transform_data(**kwargs):
        """
        Transforms the fetched cryptocurrency data into a DataFrame and pushes it to XCom.
        """
        ti = kwargs['ti']
        fetched_data = ti.xcom_pull(task_ids='fetch_crypto_prices', key='fetched_data')
        if not fetched_data:
            raise ValueError("No data fetched from the API.")

        # Convert JSON string back to Python list of dictionaries
        crypto_data = json.loads(fetched_data)

        # Create a Pandas DataFrame
        df = pd.DataFrame(crypto_data)

        # Push transformed data to XCom
        ti.xcom_push(key='transformed_data', value=df.to_json(orient='records'))

    # Task to load transformed data into PostgreSQL
    def load_to_postgres(**kwargs):
        """
        Load transformed data into PostgreSQL using PostgresHook's bulk insert.
        """
        try:
            # Pull transformed data from XCom
            ti = kwargs['ti']
            transformed_data = ti.xcom_pull(task_ids='transform_data', key='transformed_data')
            df = pd.read_json(transformed_data, orient='records')

            # Initialize PostgresHook
            pg_hook = PostgresHook(postgres_conn_id="postgres_crypto")  # Replace with your connection ID
            conn = pg_hook.get_conn()
            cursor = conn.cursor()

            # Convert DataFrame to list of tuples (matches table schema)
            data_tuples = [tuple(x) for x in df.to_numpy()]

            # Use PostgreSQL COPY command for efficient bulk insert
            insert_sql = """
                INSERT INTO crypto_prices (
                    symbol, price_usd, change_24h
                ) VALUES (%s, %s, %s)
            """

            # Batch insert with execute_batch
            execute_batch(cursor, insert_sql, data_tuples, page_size=100)
            conn.commit()
            print(f"Successfully inserted {len(df)} records")

        except Exception as e:
            conn.rollback()
            print(f"Error loading data: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()

    # Define tasks
    fetch_prices_task = PythonOperator(
        task_id='fetch_crypto_prices',
        python_callable=fetch_crypto_prices,
        provide_context=True,  # Enable context passing between tasks
        dag=dag,
    )

    transform_data_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True,  # Enable context passing between tasks
        dag=dag,
    )

    load_to_postgres_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
        provide_context=True,  # Enable context passing between tasks
        dag=dag,
    )

    # Set task dependencies
    fetch_prices_task >> transform_data_task >> load_to_postgres_task