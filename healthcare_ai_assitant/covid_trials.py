"""
COVID-19 Clinical Trials ETL Pipeline

This DAG performs weekly extraction of COVID-19 treatment clinical trials data from clinicaltrials.gov API,
transforms the data, and loads it into a PostgreSQL database.

Key Features:
- Runs every Monday at 6 AM
- Sends email notifications on failure
- Uses XCom to pass data between tasks
- Implements proper error handling
- Maintains data integrity with UPSERT operations
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email
import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

default_args = {
    'owner': 'Ikengr',
    'depends_on_past': False,
    'email': ['ikechukwu.elijah@yahoo.com'],  
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=15),
    'start_date': datetime(2025, 4, 18),
}

# Define the DAG
dag = DAG(
    'covid_clinical_trials_etl',
    default_args=default_args,
    description='Weekly ETL pipeline for COVID-19 clinical trials data from clinicaltrials.gov to PostgreSQL',
    schedule_interval='0 6 * * 1',  # At 06:00 on Monday (CRON syntax)
    catchup=False,
    tags=['clinical_trials', 'covid19', 'etl'],
)

def notify_failure(context):
    """Custom email notification on failure"""
    task_instance = context['task_instance']
    subject = f"DAG {context['dag'].dag_id} failed on task {task_instance.task_id}"
    body = f"""
    Task {task_instance.task_id} in DAG {context['dag'].dag_id} failed.
    
    Error: {context['exception']}
    
    Execution Date: {context['ds']}
    """
    send_email(to=default_args['email'], subject=subject, html_content=body)

def fetch_clinical_trials_data(**kwargs):
    """
    Extracts clinical trials data from clinicaltrials.gov API
    
    Returns:
        JSON string of DataFrame containing:
        - NCT Number (unique identifier)
        - Study Title
        - Study Status
        - List of Interventions
    
    Raises:
        Exception: If API request fails or data parsing fails
    """
    try:
        base_url = "https://clinicaltrials.gov/api/v2/studies"
        params = {
            "query.cond": "COVID-19",
            "query.term": "treatment",
            "format": "json",
            "pageSize": 100  # Max records per page
        }
        headers = {"User-Agent": "Mozilla/5.0"}
        all_studies = []
        next_page_token = None

        # Paginate through all available results
        while True:
            if next_page_token:
                params["pageToken"] = next_page_token
            else:
                params.pop("pageToken", None)

            response = requests.get(base_url, params=params, headers=headers)
            response.raise_for_status()  # Raises HTTPError for bad responses
            data = response.json()

            # Extract relevant study information
            studies = data.get("studies", [])
            for study in studies:
                protocol = study.get("protocolSection", {})
                interventions = protocol.get("interventionModule", {}).get("interventions", [])
                
                all_studies.append({
                    "NCT Number": protocol.get("identificationModule", {}).get("nctId"),
                    "Title": protocol.get("identificationModule", {}).get("briefTitle"),
                    "Status": protocol.get("statusModule", {}).get("overallStatus"),
                    "Interventions": [i.get("name") for i in interventions]
                })

            next_page_token = data.get("nextPageToken")
            if not next_page_token:
                break  # Exit loop when no more pages

        df = pd.DataFrame(all_studies)
        print(f"Successfully fetched {len(df)} clinical trials records")
        
        # Push to XCom for downstream tasks
        kwargs['ti'].xcom_push(key='clinical_trials_data', value=df.to_json())
        return df.to_json()

    except Exception as e:
        error_msg = f"Failed to fetch clinical trials data: {str(e)}"
        print(error_msg)
        raise Exception(error_msg)

def load_to_postgres(**kwargs):
    """
    Loads clinical trials data into PostgreSQL database
    
    Steps:
    1. Retrieves data from XCom
    2. Establishes DB connection
    3. Creates table if not exists
    4. Performs UPSERT operation
    
    Raises:
        Exception: If database operation fails
    """
    ti = kwargs['ti']
    
    try:
        # Retrieve data from XCom
        clinical_trials_json = ti.xcom_pull(task_ids='fetch_data', key='clinical_trials_data')
        df = pd.read_json(clinical_trials_json)

        # Establish database connection
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        print("Successfully connected to PostgreSQL database")

        # Create table if not exists
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS covid_clinical_trials (
            nct_number TEXT PRIMARY KEY,
            title TEXT,
            status TEXT,
            interventions TEXT[],
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cursor.execute(create_table_sql)

        # Prepare data for UPSERT operation
        values = [
            (
                row["NCT Number"],
                row["Title"],
                row["Status"],
                row["Interventions"]
            )
            for _, row in df.iterrows()
        ]

        # Execute batch insert with conflict handling
        insert_sql = """
        INSERT INTO covid_clinical_trials (nct_number, title, status, interventions)
        VALUES %s
        ON CONFLICT (nct_number) DO UPDATE SET
            title = EXCLUDED.title,
            status = EXCLUDED.status,
            interventions = EXCLUDED.interventions,
            last_updated = CURRENT_TIMESTAMP;
        """
        execute_values(cursor, insert_sql, values)
        conn.commit()
        
        print(f"Successfully loaded/updated {len(values)} records in covid_clinical_trials table")

    except Exception as e:
        error_msg = f"Database operation failed: {str(e)}"
        print(error_msg)
        if 'conn' in locals():
            conn.rollback()
        raise Exception(error_msg)
    finally:
        if 'conn' in locals():
            cursor.close()
            conn.close()
            print("Database connection closed")

# Define tasks
fetch_data = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_clinical_trials_data,
    provide_context=True,
    on_failure_callback=notify_failure,
    dag=dag,
)

load_data = PythonOperator(
    task_id='load_data',
    python_callable=load_to_postgres,
    provide_context=True,
    on_failure_callback=notify_failure,
    dag=dag,
)

# Set task dependencies
fetch_data >> load_data