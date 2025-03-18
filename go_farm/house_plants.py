from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import pandas as pd
import json
from datetime import datetime, timedelta

# API Configuration
API_URL = "https://house-plants2.p.rapidapi.com/all-lite"
HEADERS = {
    "x-rapidapi-key": "f38eae887bmsh5211e33c97c1c50p125cafjsnec52eb060a05",
    "x-rapidapi-host": "house-plants2.p.rapidapi.com"
}

# Default DAG arguments
default_args = {
    "owner": "Ik",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 18),  # Adjust start date
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    "house_plants_etl",
    default_args=default_args,
    description="Fetch house plants data and load into PostgreSQL",
    schedule_interval="0 0 * * 0",  # Runs every Sunday at midnight
    catchup=False
)

def fetch_data(**kwargs):
    """Fetch data from the API and push it to XCom."""
    response = requests.get(API_URL, headers=HEADERS)
    data = response.json()
    kwargs['ti'].xcom_push(key='plant_data', value=data)

def transform_data(**kwargs):
    """Transform raw API data into a structured format."""
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='fetch_data', key='plant_data')

    # Transform data to DataFrame
    df = pd.DataFrame([{
        'category': plant.get('Categories', ''),
        'common_name': ', '.join(plant.get('Common name', [])) if plant.get('Common name') else None,
        'latin_name': plant.get('Latin name', ''),
        'family': plant.get('Family', ''),
        'origin': ', '.join(plant.get('Origin', [])) if plant.get('Origin') else None,
        'climate': plant.get('Climat', ''),
        'image_url': plant.get('Img', ''),
        'zone': ', '.join(plant.get('Zone', [])) if plant.get('Zone') else None
    } for plant in data])

    transformed_data = df.to_dict(orient="records")  # Convert DataFrame to list of dictionaries
    ti.xcom_push(key='transformed_data', value=transformed_data)

def load_data(**kwargs):
    """Load transformed data into PostgreSQL using PostgresHook."""
    ti = kwargs['ti']
    records = ti.xcom_pull(task_ids='transform_data', key='transformed_data')

    if not records:
        print("No data to load.")
        return

    # Define Postgres connection
    pg_hook = PostgresHook(postgres_conn_id="postgres_dwh")

    # Ensure the table exists
    create_table_query = """
    CREATE TABLE IF NOT EXISTS house_plants (
        id SERIAL PRIMARY KEY,
        category TEXT,
        common_name TEXT,
        latin_name TEXT,
        family TEXT,
        origin TEXT,
        climate TEXT,
        image_url TEXT,
        zone TEXT,
        UNIQUE (latin_name, zone)  -- Prevent duplicate inserts
    );
    """
    pg_hook.run(create_table_query)

    # Insert data into PostgreSQL
    insert_query = """
    INSERT INTO house_plants (category, common_name, latin_name, family, origin, climate, image_url, zone)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (latin_name, zone) DO NOTHING;
    """
    values = [(record['category'], record['common_name'], record['latin_name'], record['family'], 
               record['origin'], record['climate'], record['image_url'], record['zone']) for record in records]

    pg_hook.insert_rows("house_plants", values, target_fields=['category', 'common_name', 'latin_name', 'family', 'origin', 'climate', 'image_url', 'zone'])

    print("Data successfully loaded into PostgreSQL!")

# Define Tasks
fetch_task = PythonOperator(
    task_id="fetch_data",
    python_callable=fetch_data,
    provide_context=True,
    dag=dag
)

transform_task = PythonOperator(
    task_id="transform_data",
    python_callable=transform_data,
    provide_context=True,
    dag=dag
)

load_task = PythonOperator(
    task_id="load_data",
    python_callable=load_data,
    provide_context=True,
    dag=dag
)

# Task Dependencies
fetch_task >> transform_task >> load_task
