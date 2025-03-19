from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import pandas as pd
import json
from datetime import datetime, timedelta

# Default DAG arguments
default_args = {
    "owner": "Ik",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 18),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Initialize DAG
dag = DAG(
    "amazon_best_sellers_st",
    default_args=default_args,
    description="Extract data from Amazon API and load into PostgreSQL",
    schedule_interval="0 0 * * 0",  # Runs weekly on Sunday at midnight
    catchup=False,
)

# Step 1: Extract data from API
def extract_data(**kwargs):
    url = "https://real-time-amazon-data.p.rapidapi.com/search"
    querystring = {
        "query": "sex_toys",
        "page": "1",
        "country": "US",
        "sort_by": "BEST_SELLERS",
        "product_condition": "ALL",
        "is_prime": "false",
        "deals_and_discounts": "NONE"
    }
    headers = {
        "x-rapidapi-key": "f38eae887bmsh5211e33c97c1c50p125cafjsnec52eb060a05",
        "x-rapidapi-host": "real-time-amazon-data.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)
    data = response.json()

    # Push raw data to XCom
    kwargs['ti'].xcom_push(key="raw_data", value=data)

extract_task = PythonOperator(
    task_id="extract_data",
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)

# Step 2: Transform data
def transform_data(**kwargs):
    ti = kwargs['ti']
    raw_data = ti.xcom_pull(task_ids="extract_data", key="raw_data")

    # Extract product data
    products = raw_data.get("data", {}).get("products", [])
    df = pd.DataFrame(products)

    # Select required columns
    df = df[["asin", "product_title", "product_price", "product_star_rating", 
             "product_num_ratings", "product_url", "product_photo", "sales_volume", "delivery"]]

    # Convert DataFrame to JSON string for XCom
    transformed_data = df.to_json(orient="records")
    ti.xcom_push(key="transformed_data", value=transformed_data)

transform_task = PythonOperator(
    task_id="transform_data",
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

# Step 3: Load data into PostgreSQL
def load_data(**kwargs):
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids="transform_data", key="transformed_data")
    df = pd.read_json(transformed_data)

    # Connect to PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id="postgres_dwh")  # Ensure you have this connection set up in Airflow
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Create table if not exists
    create_table_query = """
    CREATE TABLE IF NOT EXISTS amazon_sex_toy_products (
        asin TEXT PRIMARY KEY,
        product_title TEXT,
        product_price TEXT,
        product_star_rating TEXT,
        product_num_ratings INTEGER,
        product_url TEXT,
        product_photo TEXT,
        sales_volume TEXT,
        delivery TEXT
    );
    """
    cursor.execute(create_table_query)
    conn.commit()

    # Insert data
    for _, row in df.iterrows():
        insert_query = """
        INSERT INTO amazon_sex_toy_products (
            asin, product_title, product_price, product_star_rating, 
            product_num_ratings, product_url, product_photo, sales_volume, delivery
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (asin) DO NOTHING;
        """
        values = (
            row["asin"], row["product_title"], row["product_price"],
            row["product_star_rating"], row["product_num_ratings"],
            row["product_url"], row["product_photo"], row["sales_volume"],
            row["delivery"]
        )
        cursor.execute(insert_query, values)

    # Commit and close connection
    conn.commit()
    cursor.close()
    conn.close()

load_task = PythonOperator(
    task_id="load_data",
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

# Define DAG flow
extract_task >> transform_task >> load_task
