from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import requests
import pandas as pd

# Define default arguments for the DAG
default_args = {
    'owner': 'Ik',
    'depends_on_past': False,
    'start_date': days_ago(1),  # Runs from yesterday
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG with a weekly schedule
dag = DAG(
    'ca_job_postings',
    default_args=default_args,
    description='Fetch job postings from API and load into PostgreSQL using XCom',
    schedule_interval="@weekly",  # Runs every week
    catchup=False
)

# Task 1: Fetch data from API and store in XCom
def fetch_data_from_api(**kwargs):
    url = "https://job-posting-feed-api.p.rapidapi.com/active-ats-meili"
    querystring = {
        "search": "Data Engineer",
        "title_search": "true",
        "description_type": "html",
        "location_filter": "Canada"
    }
    headers = {
        "x-rapidapi-key": "b138837176msh66610dbb568050ap1aec3ejsn32d522c6ac16",
        "x-rapidapi-host": "job-posting-feed-api.p.rapidapi.com"
    }
    
    response = requests.get(url, headers=headers, params=querystring)

    if response.status_code == 200:
        data = response.json()
        kwargs['ti'].xcom_push(key='job_postings_raw', value=data['hits'])
        return "Data fetched and stored in XCom"
    else:
        raise Exception(f"API request failed with status code {response.status_code}")

# Task 2: Transform data using Pandas
def transform_data(**kwargs):
    ti = kwargs['ti']
    raw_data = ti.xcom_pull(task_ids='fetch_data_from_api', key='job_postings_raw')

    if not raw_data:
        raise ValueError("No data received from XCom")

    # Convert JSON to DataFrame
    job_postings = []
    for job in raw_data:
        job_postings.append({
            'job_title': job.get('title', ''),
            'locations': ', '.join(job.get('locations_derived', [''])),
            'date_posted': job.get('date_posted', None)
        })

    df = pd.DataFrame(job_postings)

    # Convert DataFrame to list of dictionaries for XCom
    transformed_data = df.to_dict(orient='records')
    ti.xcom_push(key='transformed_job_data', value=transformed_data)

    return "Data transformed and stored in XCom"

# Task 3: Load data into PostgreSQL using PostgresHook
def load_data_to_postgres(**kwargs):
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform_data', key='transformed_job_data')

    if not transformed_data:
        raise ValueError("No transformed data received from XCom")

    # Establish PostgreSQL connection using PostgresHook
    postgres_hook = PostgresHook(postgres_conn_id="postgres_dwh")
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # Create table if not exists
    create_table_query = """
    CREATE TABLE IF NOT EXISTS job_postings (
        id SERIAL PRIMARY KEY,
        job_title TEXT,
        locations TEXT,
        date_posted TIMESTAMP,
        CONSTRAINT unique_job_posting UNIQUE (job_title, locations, date_posted)
    );
    """
    cursor.execute(create_table_query)
    conn.commit()

    # Insert data into table
    insert_query = """
    INSERT INTO job_postings (job_title, locations, date_posted)
    VALUES (%s, %s, %s)
    ON CONFLICT (job_title, locations, date_posted) DO NOTHING;
    """

    # Convert transformed data into tuples for insertion
    records = [(job['job_title'], job['locations'], job['date_posted']) for job in transformed_data]

    cursor.executemany(insert_query, records)
    conn.commit()

    cursor.close()
    conn.close()
    return "Data loaded into PostgreSQL successfully"

# Define Airflow tasks
fetch_task = PythonOperator(
    task_id='fetch_data_from_api',
    python_callable=fetch_data_from_api,
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
    task_id='load_data_to_postgres',
    python_callable=load_data_to_postgres,
    provide_context=True,
    dag=dag
)

# Define task dependencies
fetch_task >> transform_task >> load_task
