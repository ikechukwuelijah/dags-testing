from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import pandas as pd
import json
from datetime import datetime

# Define default arguments for the DAG
default_args = {
    "owner": "Ik",
    "start_date": datetime(2025, 3, 11),
    "retries": 1
}

# Define DAG
dag = DAG(
    "amazon_fashion_etl",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
)

# API details
url = "https://real-time-amazon-data.p.rapidapi.com/search"
querystring = {
    "query": "Fashion",
    "page": "1",
    "country": "US",
    "sort_by": "BEST_SELLERS",
    "product_condition": "ALL",
    "is_prime": "false",
    "deals_and_discounts": "NONE"
}
headers = {
    "x-rapidapi-key": "b138837176msh66610dbb568050ap1aec3ejsn32d522c6ac16",
    "x-rapidapi-host": "real-time-amazon-data.p.rapidapi.com"
}

# Step 1: Extract data from API
def extract_data(**kwargs):
    """Fetch data from API and push to XCom"""
    response = requests.get(url, headers=headers, params=querystring)
    data = response.json()
    kwargs["ti"].xcom_push(key="raw_data", value=data)  # Store raw data in XCom
    print("Data extracted successfully.")

# Step 2: Transform JSON to DataFrame
def transform_data(**kwargs):
    """Convert raw JSON data to DataFrame and push to XCom"""
    ti = kwargs["ti"]
    raw_data = ti.xcom_pull(task_ids="extract_task", key="raw_data")

    if 'data' in raw_data and 'products' in raw_data['data']:
        df = pd.DataFrame(raw_data['data']['products'])
        ti.xcom_push(key="transformed_data", value=df.to_json())  # Store DataFrame as JSON
        print("Data transformed successfully.")
    else:
        print("No product data found in the response.")

# Step 3: Load Data into PostgreSQL
def load_to_postgres(**kwargs):
    """Load transformed data into PostgreSQL using PostgresHook"""
    ti = kwargs["ti"]
    df_json = ti.xcom_pull(task_ids="transform_task", key="transformed_data")

    if df_json:
        df = pd.read_json(df_json)

        # Connect to PostgreSQL using PostgresHook
        pg_hook = PostgresHook(postgres_conn_id="postgres_dwh")  # Airflow connection ID
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Create table if not exists
        create_table_query = """
        CREATE TABLE IF NOT EXISTS fashion_products (
            id SERIAL PRIMARY KEY,
            title TEXT,
            price TEXT,
            rating FLOAT,
            reviews TEXT,
            product_url TEXT
        );
        """
        cursor.execute(create_table_query)

        # Insert data into the table
        insert_query = """
        INSERT INTO fashion_products (title, price, rating, reviews, product_url)
        VALUES (%s, %s, %s, %s, %s);
        """
        for _, row in df.iterrows():
            cursor.execute(insert_query, (
                row.get("title"),
                row.get("price"),
                row.get("rating"),
                row.get("reviews"),
                row.get("product_url")
            ))

        # Commit changes and close connection
        conn.commit()
        cursor.close()
        conn.close()
        print("Data loaded successfully.")
    else:
        print("No transformed data found.")

# Define tasks
extract_task = PythonOperator(
    task_id="extract_task",
    python_callable=extract_data,
    provide_context=True,
    dag=dag
)

transform_task = PythonOperator(
    task_id="transform_task",
    python_callable=transform_data,
    provide_context=True,
    dag=dag
)

load_task = PythonOperator(
    task_id="load_task",
    python_callable=load_to_postgres,
    provide_context=True,
    dag=dag
)

# Set task dependencies
extract_task >> transform_task >> load_task
