from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import requests
import pandas as pd
import os

# Default arguments for the DAG
default_args = {
    'owner': 'Ik',
    'start_date': datetime(2025, 3, 4),
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'metal_prices',
    default_args=default_args,
    description='A simple DAG to load metal prices data',
    schedule_interval='0 */6 * * *',  # Run four times daily
    catchup=False,
)

def fetch_data_from_api():
    url = "https://gold-price-live.p.rapidapi.com/get_metal_prices"
    headers = {
        "x-rapidapi-key": os.getenv("RAPIDAPI_KEY"),  # Use environment variable for security
        "x-rapidapi-host": "gold-price-live.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()  # Example: {'gold': 2918.21, 'silver': 31.8791}
        return data
    else:
        raise Exception(f"Error: {response.status_code}, {response.text}")

def transform_data(data):
    df = pd.DataFrame(list(data.items()), columns=['metal_name', 'price'])
    df["currency"] = "USD"  # Assuming all prices are in USD
    return df

def load_data_to_postgres(df):
    # Use PostgresHook to connect to the PostgreSQL database
    pg_hook = PostgresHook(postgres_conn_id='postgres_dwh')

    # Establish a connection
    conn = pg_hook.get_conn()
    cur = conn.cursor()

    # Create table if not exists
    create_table_query = """
    CREATE TABLE IF NOT EXISTS metal_prices (
        metal_name VARCHAR(50),
        price FLOAT,
        currency VARCHAR(3)
    )
    """
    cur.execute(create_table_query)
    conn.commit()

    # Insert data into the table
    for index, row in df.iterrows():
        insert_query = """
        INSERT INTO metal_prices (metal_name, price, currency)
        VALUES (%s, %s, %s)
        """
        cur.execute(insert_query, (row['metal_name'], row['price'], row['currency']))

    conn.commit()
    cur.close()
    conn.close()

def main():
    data = fetch_data_from_api()
    df = transform_data(data)
    load_data_to_postgres(df)

# Define the PythonOperator tasks
fetch_data_task = PythonOperator(
    task_id='fetch_data_from_api',
    python_callable=fetch_data_from_api,
    dag=dag,
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=lambda: transform_data(fetch_data_task.output),
    dag=dag,
)

load_data_task = PythonOperator(
    task_id='load_data_to_postgres',
    python_callable=lambda: load_data_to_postgres(transform_data_task.output),
    dag=dag,
)

# Set task dependencies
fetch_data_task >> transform_data_task >> load_data_task
