from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import pandas as pd
import json
from datetime import datetime

def fetch_data(**kwargs):
    url = "https://job-posting-feed-api.p.rapidapi.com/active-ats-meili"
    querystring = {
        "search": "Data Engineer",
        "title_search": "true",
        "description_type": "html",
        "location_filter": "United Kingdom"
    }
    headers = {
        "x-rapidapi-key": "f38eae887bmsh5211e33c97c1c50p125cafjsnec52eb060a05",
        "x-rapidapi-host": "job-posting-feed-api.p.rapidapi.com"
    }
    response = requests.get(url, headers=headers, params=querystring)
    data = response.json()
    kwargs['ti'].xcom_push(key='raw_data', value=json.dumps(data))

def process_data(**kwargs):
    ti = kwargs['ti']
    raw_data = json.loads(ti.xcom_pull(task_ids='fetch_data', key='raw_data'))
    
    if 'hits' in raw_data and isinstance(raw_data['hits'], list):
        job_postings = raw_data['hits']
        df = pd.DataFrame([{ 
            'job_title': job.get('title', ''), 
            'locations': ', '.join(job.get('locations_derived', [''])), 
            'date_posted': job.get('date_posted', None) 
        } for job in job_postings])
    else:
        df = pd.DataFrame()
    
    ti.xcom_push(key='processed_data', value=df.to_json())

def load_to_postgres(**kwargs):
    ti = kwargs['ti']
    df_json = ti.xcom_pull(task_ids='process_data', key='processed_data')
    df = pd.read_json(df_json)
    
    if not df.empty:
        hook = PostgresHook(postgres_conn_id='postgres_dwh')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS uk_de_job_postings (
                id SERIAL PRIMARY KEY,
                job_title TEXT,
                locations TEXT,
                date_posted TIMESTAMP,
                CONSTRAINT uk_de_job_postings_unique UNIQUE (job_title, locations, date_posted)
            );
        """)
        conn.commit()
        
        insert_query = """
            INSERT INTO uk_de_job_postings (job_title, locations, date_posted)
            VALUES (%s, %s, %s)
            ON CONFLICT (job_title, locations, date_posted) DO NOTHING;
        """
        records = df.replace({pd.NA: None}).values.tolist()
        cursor.executemany(insert_query, records)
        conn.commit()
        
        cursor.close()
        conn.close()

default_args = {
    'owner': 'Ik',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 18),
    'retries': 1,
}

dag = DAG(
    'uk_de_job_postings',
    default_args=default_args,
    description='Fetch, process, and store job postings data weekly',
    schedule_interval='@weekly',
    catchup=False
)

t1 = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_data,
    provide_context=True,
    dag=dag
)

t2 = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    provide_context=True,
    dag=dag
)

t3 = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    provide_context=True,
    dag=dag
)

t1 >> t2 >> t3
