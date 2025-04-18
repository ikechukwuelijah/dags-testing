"""
COVID-19 Clinical Trials ETL Pipeline with Email Notifications
"""

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
    'email': ['ikechukwu.elijah@yahoo.com'],  # Your email address
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 1, 1),
}

def notify_failure(context):
    """Custom email notification on failure"""
    task_instance = context['task_instance']
    dag_id = context['dag'].dag_id
    task_id = task_instance.task_id
    exception = context.get('exception')
    execution_date = context.get('execution_date')
    
    subject = f"Airflow DAG Failed: {dag_id}"
    body = f"""
    <h2>DAG Failure Alert</h2>
    <p><strong>DAG:</strong> {dag_id}</p>
    <p><strong>Failed Task:</strong> {task_id}</p>
    <p><strong>Execution Time:</strong> {execution_date}</p>
    <p><strong>Error:</strong></p>
    <pre>{str(exception)}</pre>
    """
    
    send_email(to=default_args['email'], subject=subject, html_content=body)

dag = DAG(
    'covid_clinical_trials_etl',
    default_args=default_args,
    description='ETL pipeline for COVID-19 clinical trials with email notifications',
    schedule_interval='0 6 * * 1',  # Every Monday at 6 AM
    catchup=False,
    on_failure_callback=notify_failure,  # Custom failure notification
)

def create_table(**kwargs):
    """Ensure the target table exists with correct schema"""
    try:
        hook = PostgresHook(postgres_conn_id='postgres_dwh')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        create_sql = """
        CREATE TABLE IF NOT EXISTS clinical_trials (
            nct_id TEXT PRIMARY KEY,
            title TEXT,
            status TEXT,
            interventions TEXT[],
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cursor.execute(create_sql)
        conn.commit()
        print("Table created/verified successfully")
        
    except Exception as e:
        error_msg = f"Table creation failed: {str(e)}"
        print(error_msg)
        raise Exception(error_msg)
    finally:
        if 'conn' in locals():
            cursor.close()
            conn.close()

def fetch_data(**kwargs):
    """Extract data from API"""
    try:
        url = "https://clinicaltrials.gov/api/v2/studies"
        params = {
            "query.cond": "COVID-19",
            "query.term": "treatment",
            "format": "json",
            "pageSize": 100
        }
        
        response = requests.get(url, params=params)
        response.raise_for_status()
        raw_data = response.json()
        
        kwargs['ti'].xcom_push(key='raw_data', value=raw_data)
        print(f"Fetched {len(raw_data.get('studies', []))} records")
        return raw_data
        
    except Exception as e:
        error_msg = f"Data fetch failed: {str(e)}"
        print(error_msg)
        raise Exception(error_msg)

def transform_data(**kwargs):
    """Transform raw data into structured format"""
    try:
        ti = kwargs['ti']
        raw_data = ti.xcom_pull(task_ids='fetch_data', key='raw_data')
        
        studies = raw_data.get("studies", [])
        transformed = []
        
        for study in studies:
            protocol = study.get("protocolSection", {})
            transformed.append({
                "nct_id": protocol.get("identificationModule", {}).get("nctId"),
                "title": protocol.get("identificationModule", {}).get("briefTitle"),
                "status": protocol.get("statusModule", {}).get("overallStatus"),
                "interventions": [
                    i.get("name") for i in 
                    protocol.get("interventionModule", {}).get("interventions", [])
                ]
            })
        
        kwargs['ti'].xcom_push(key='transformed_data', value=transformed)
        print(f"Transformed {len(transformed)} records")
        return transformed
        
    except Exception as e:
        error_msg = f"Data transformation failed: {str(e)}"
        print(error_msg)
        raise Exception(error_msg)

def load_data(**kwargs):
    """Load transformed data to PostgreSQL"""
    try:
        ti = kwargs['ti']
        transformed_data = ti.xcom_pull(task_ids='transform_data', key='transformed_data')
        
        hook = PostgresHook(postgres_conn_id='postgres_dwh')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        execute_values(
            cursor,
            """INSERT INTO clinical_trials (nct_id, title, status, interventions)
               VALUES %s ON CONFLICT (nct_id) DO UPDATE SET
               title = EXCLUDED.title,
               status = EXCLUDED.status,
               interventions = EXCLUDED.interventions,
               last_updated = CURRENT_TIMESTAMP""",
            [(d['nct_id'], d['title'], d['status'], d['interventions']) for d in transformed_data]
        )
        conn.commit()
        print(f"Loaded {len(transformed_data)} records")
        
    except Exception as e:
        error_msg = f"Data load failed: {str(e)}"
        print(error_msg)
        if 'conn' in locals():
            conn.rollback()
        raise Exception(error_msg)
    finally:
        if 'conn' in locals():
            cursor.close()
            conn.close()

# Define tasks
create_table_task = PythonOperator(
    task_id='create_table',
    python_callable=create_table,
    dag=dag,
)

fetch_task = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

# Set dependencies
create_table_task >> fetch_task >> transform_task >> load_task
