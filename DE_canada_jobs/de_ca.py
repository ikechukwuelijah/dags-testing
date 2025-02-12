from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.utils.email import send_email
import requests
import json
import pandas as pd
from sqlalchemy import create_engine

# ====================================================
# 1. AIRFLOW SETUP AND CONFIGURATION
# ====================================================

# Default arguments for the DAG
default_args = {
    'owner': 'Ik',  # Owner of the DAG
    'depends_on_past': False,  # Do not depend on past runs
    'start_date': datetime(2025, 2, 12),  # Start date for the DAG
}

# Instantiate the DAG
dag = DAG(
    'canada_DEjobs',  # DAG ID
    default_args=default_args,
    description='ETL pipeline for LinkedIn Data Engineer jobs in Canada',
    schedule_interval='@daily',  # Runs once per day
    catchup=False,  # Do not backfill past runs
    tags=['etl', 'linkedin', 'jobs']  # Tags for categorization
)

# ====================================================
# 2. HELPER FUNCTIONS FOR TASKS
# ====================================================

def fetch_data(**kwargs):
    """
    Task to fetch data from LinkedIn API.
    Uses Airflow Variables for sensitive credentials.
    Pushes raw data to XCom for downstream tasks.
    """
    try:
        # API configuration
        url = "https://linkedin-job-search-api.p.rapidapi.com/active-jb-24h"
        querystring = {
            "title_filter": "Data Engineer",
            "location_filter": "Canada",
            "remote": "false",
            "agency": "false",
            "Count": 50  
        }
        headers = {
            "x-rapidapi-key": Variable.get("rapidapi_key"),  # Fetch API key from Airflow Variables
            "x-rapidapi-host": "linkedin-jobs-api2.p.rapidapi.com"
        }

        # Execute API request
        response = requests.get(url, headers=headers, params=querystring)
        response.raise_for_status()  # Raise exception for HTTP errors

        # Parse JSON response
        data = response.json()

        # Push raw data to XCom for downstream tasks
        kwargs['ti'].xcom_push(key='raw_data', value=data)
        print(f"Fetched {len(data)} job listings.")
    except Exception as e:
        print(f"Error fetching data: {str(e)}")
        raise

    def transform_job_data(**kwargs):
        """
        Processes raw JSON data into structured DataFrame
        Returns DataFrame as JSON string through XCom
        """
        ti = kwargs['ti']
        json_data = ti.xcom_pull(task_ids='fetch_job_data', key='raw_job_data')
        
        if not json_data:
            raise ValueError("No data received from fetch task")
        
        # Normalize JSON data
        df = pd.json_normalize(json_data)
        
        # Handle list-type columns
        list_columns = ['title_keywords', 'linkedin_org_locations', 'locations_raw']
        for col in list_columns:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else None)
        
        # Convert datetime columns
        datetime_columns = ['date_posted', 'date_created', 'date_validthrough']
        for col in datetime_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Clean text columns
        if 'linkedin_org_description' in df.columns:
            df['linkedin_org_description'] = df['linkedin_org_description'].str.replace('\u202f', ' ', regex=True)
        
        # Select relevant columns
        columns_order = [
            'id', 'title', 'organization', 'seniority',
            'date_posted', 'date_created', 'date_validthrough',
            'locations_raw', 'linkedin_org_locations',
            'linkedin_org_description', 'organization_url',
            'linkedin_org_recruitment_agency_derived', 'title_keywords'
        ]
        final_columns = [col for col in columns_order if col in df.columns]
        df = df[final_columns]
        
        # Return DataFrame as JSON string
        return df.to_json(orient='records', date_format='iso')

    def load_job_data(**kwargs):
        """
        Loads transformed data into PostgreSQL database
        """
        ti = kwargs['ti']
        json_str = ti.xcom_pull(task_ids='transform_job_data')
        
        if not json_str:
            raise ValueError("No data received from transform task")
        
        # Convert JSON string back to DataFrame
        df = pd.read_json(json_str, orient='records', convert_dates=['date_posted', 'date_created', 'date_validthrough'])
        
        # Get PostgreSQL connection
        pg_hook = PostgresHook(postgres_conn_id='postgres_dwh')
        engine = pg_hook.get_sqlalchemy_engine()
        
        try:
            if not df.empty:
                # Load data into PostgreSQL
                df.to_sql(
                    name="DE_canada_jobs",
                    con=engine,
                    schema="public",
                    if_exists="append",
                    index=False
                )
                print(f"Loaded {len(df)} records successfully")
            else:
                print("Empty DataFrame - nothing to load")
        except Exception as e:
            raise Exception(f"Database operation failed: {str(e)}")
        finally:
            engine.dispose()

    # Define tasks
    fetch_task = PythonOperator(
        task_id='fetch_job_data',
        python_callable=fetch_job_data,
    )

    transform_task = PythonOperator(
        task_id='transform_job_data',
        python_callable=transform_job_data,
    )

    load_task = PythonOperator(
        task_id='load_job_data',
        python_callable=load_job_data,
    )

    # Set task dependencies
    fetch_task >> transform_task >> load_task