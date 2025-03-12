from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import pandas as pd

# Define default arguments
default_args = {
    'owner': 'Ik',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 12),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'amazon_home_kitchen_etl',
    default_args=default_args,
    description='Daily ETL for Amazon Home & Kitchen Products',
    schedule_interval='@daily',
    catchup=False
)

# Step 1: Extract Data
API_URL = "https://real-time-amazon-data.p.rapidapi.com/search"
QUERY_PARAMS = {
    "query": "Home & Kitchen",
    "page": "1",
    "country": "US",
    "sort_by": "BEST_SELLERS",
    "product_condition": "ALL",
    "is_prime": "false",
    "deals_and_discounts": "NONE"
}
HEADERS = {
    "x-rapidapi-key": "b138837176msh66610dbb568050ap1aec3ejsn32d522c6ac16",
    "x-rapidapi-host": "real-time-amazon-data.p.rapidapi.com"
}

def extract_data(**kwargs):
    response = requests.get(API_URL, headers=HEADERS, params=QUERY_PARAMS)
    data = response.json()
    kwargs['ti'].xcom_push(key='api_data', value=data)

# Step 2: Transform Data
def transform_data(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract_data', key='api_data')
    
    if "data" in data and "products" in data["data"]:
        df = pd.DataFrame(data["data"]["products"])
    else:
        df = pd.DataFrame(columns=["title", "price", "rating", "reviews", "url"])
    
    if "url" in df.columns:
        df.rename(columns={"url": "product_url"}, inplace=True)
    
    for col in ["title", "price", "rating", "reviews", "product_url"]:
        if col not in df.columns:
            df[col] = None
    
    ti.xcom_push(key='processed_data', value=df.to_json())

# Step 3: Load Data
def load_to_postgres(**kwargs):
    ti = kwargs['ti']
    df_json = ti.xcom_pull(task_ids='transform_data', key='processed_data')
    df = pd.read_json(df_json)
    
    if df.empty:
        return
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_dwh')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    create_table_query = """
    CREATE TABLE IF NOT EXISTS home_kitchen_products (
        id SERIAL PRIMARY KEY,
        title TEXT,
        price TEXT,
        rating FLOAT,
        reviews TEXT,
        product_url TEXT
    );
    """
    cursor.execute(create_table_query)
    
    insert_query = """
    INSERT INTO home_kitchen_products (title, price, rating, reviews, product_url)
    VALUES (%s, %s, %s, %s, %s);
    """
    for _, row in df.iterrows():
        cursor.execute(insert_query, (row["title"], row["price"], row["rating"], row["reviews"], row["product_url"]))
    
    conn.commit()
    cursor.close()
    conn.close()

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    provide_context=True,
    dag=dag,
)

extract_task >> transform_task >> load_task