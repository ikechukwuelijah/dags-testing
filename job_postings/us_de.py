from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import pandas as pd
from datetime import datetime, timedelta

# Define default arguments
default_args = {
    'owner': 'Ik',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 19),  # Adjust start date
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize DAG
dag = DAG(
    'us_de_jobs',
    default_args=default_args,
    description='DAG to fetch, transform, and load US Data Engineer jobs into PostgreSQL',
    schedule_interval='@weekly',  # Runs every Monday at 00:00 UTC
    catchup=False
)

# Step 1: Fetch job postings from API
def fetch_job_postings(**kwargs):
    url = "https://job-posting-feed-api.p.rapidapi.com/active-ats-meili"
    querystring = {"search": "data engineer", "title_search": "false", "description_type": "html", "location_filter": "United States"}
    
    headers = {
        "x-rapidapi-key": "f38eae887bmsh5211e33c97c1c50p125cafjsnec52eb060a05",
        "x-rapidapi-host": "job-posting-feed-api.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)
    data = response.json()

    # Push data to XCom for the next task
    kwargs['ti'].xcom_push(key='raw_data', value=data)

# Step 2: Transform data
def transform_data(**kwargs):
    ti = kwargs['ti']
    raw_data = ti.xcom_pull(task_ids='fetch_job_postings', key='raw_data')

    # Convert to DataFrame
    job_list = raw_data.get('hits', [])
    df = pd.DataFrame(job_list, columns=['title', 'locations_derived', 'date_posted'])

    # Fix date_posted conversion issue
    df['date_posted'] = df['date_posted'].str.replace(r'\+00:00$', '', regex=True)  # Remove timezone
    df['date_posted'] = pd.to_datetime(df['date_posted'], errors='coerce')

    # Extract first location
    df['location'] = df['locations_derived'].apply(lambda x: x[0] if isinstance(x, list) and x else None)

    # Drop locations_derived
    df.drop(columns=['locations_derived'], inplace=True)

    # Convert to JSON for XCom
    df_json = df.to_dict(orient='records')

    # Push transformed data to XCom
    ti.xcom_push(key='transformed_data', value=df_json)

# Step 3: Load data into PostgreSQL
def load_to_postgres(**kwargs):
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform_data', key='transformed_data')

    # Initialize PostgresHook
    pg_hook = PostgresHook(postgres_conn_id='postgres_dwh')  # Ensure Airflow has this connection set up

    # Create table if not exists
    create_table_query = """
        CREATE TABLE IF NOT EXISTS us_de_jobs (
            id SERIAL PRIMARY KEY,
            title TEXT,
            location TEXT,
            date_posted DATE
        )
    """
    pg_hook.run(create_table_query)

    # Insert transformed data
    insert_query = """
        INSERT INTO us_de_jobs (title, location, date_posted)
        VALUES (%s, %s, %s)
    """
    
    records = [(row['title'], row['location'], row['date_posted']) if row['date_posted'] else None for row in transformed_data]

    pg_hook.insert_rows('us_de_jobs', records, target_fields=['title', 'location', 'date_posted'])

# Define tasks
fetch_task = PythonOperator(
    task_id='fetch_job_postings',
    python_callable=fetch_job_postings,
    provide_context=True,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    provide_context=True,
    dag=dag
)

# Task dependencies
fetch_task >> transform_task >> load_task
