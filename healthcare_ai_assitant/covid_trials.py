from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.email import send_email
import requests
import pandas as pd
from psycopg2.extras import execute_values

default_args = {
    'owner': 'Ikeengr',
    'depends_on_past': False,
    'email': ['ikechukwu.elijah@yahoo.com'],
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'covid_clinical_trials_etl',
    default_args=default_args,
    description='ETL pipeline with visible transform step',
    schedule_interval='0 6 * * 1',
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

def notify_failure(context):
    task_instance = context['task_instance']
    send_email(
        to=default_args['email'],
        subject=f"DAG failed: {context['dag'].dag_id}",
        html_content=f"Task {task_instance.task_id} failed"
    )

def fetch_data(**kwargs):
    """Extract data from API"""
    url = "https://clinicaltrials.gov/api/v2/studies"
    params = {"query.cond": "COVID-19", "format": "json"}
    
    response = requests.get(url, params=params)
    response.raise_for_status()
    raw_data = response.json()
    
    # Push raw data to XCom
    kwargs['ti'].xcom_push(key='raw_data', value=raw_data)
    return raw_data

def transform_data(**kwargs):
    """Transform raw data into structured format"""
    ti = kwargs['ti']
    raw_data = ti.xcom_pull(task_ids='fetch_data', key='raw_data')
    
    # Your transformation logic here
    studies = raw_data.get("studies", [])
    transformed = []
    
    for study in studies:
        transformed.append({
            "nct_id": study.get("protocolSection", {}).get("identificationModule", {}).get("nctId"),
            "title": study.get("protocolSection", {}).get("identificationModule", {}).get("briefTitle"),
            "status": study.get("protocolSection", {}).get("statusModule", {}).get("overallStatus"),
            "interventions": [
                i.get("name") for i in 
                study.get("protocolSection", {}).get("interventionModule", {}).get("interventions", [])
            ]
        })
    
    # Push transformed data to XCom
    kwargs['ti'].xcom_push(key='transformed_data', value=transformed)
    return transformed

def load_data(**kwargs):
    """Load transformed data to PostgreSQL"""
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform_data', key='transformed_data')
    
    hook = PostgresHook(postgres_conn_id='postgres_dwh')
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    # Your load logic here
    execute_values(
        cursor,
        """INSERT INTO clinical_trials (nct_id, title, status, interventions)
           VALUES %s ON CONFLICT (nct_id) DO UPDATE SET
           title = EXCLUDED.title, status = EXCLUDED.status""",
        [(d['nct_id'], d['title'], d['status'], d['interventions']) for d in transformed_data]
    )
    
    conn.commit()
    cursor.close()
    conn.close()

# Define tasks with explicit transform step
fetch_task = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',  # This will now appear in the graph
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

# Set dependencies to show linear flow
fetch_task >> transform_task >> load_task
