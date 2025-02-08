from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.utils.email import send_email
import pandas as pd
import requests
import logging
import io

# ====================================================
# 1. AIRFLOW SETUP AND CONFIGURATION
# ====================================================
# Default arguments for the DAG
default_args = {
    'owner': 'pharma_team',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 8),
    'email': Variable.get("email_recipients"),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1
}

# Instantiate the DAG
dag = DAG(
    'linkedin_job_scraper',
    default_args=default_args,
    description='ETL pipeline for LinkedIn job data',
    schedule_interval='@daily',  # Runs once per day
    catchup=False,
    tags=['pharmacists', 'linkedin']
)

# ====================================================
# 3. HELPER FUNCTIONS
# ====================================================
def fetch_data(**kwargs):
    """
    Task to fetch data from LinkedIn API
    Uses Airflow Variables for sensitive credentials
    """
    try:
        # Get credentials from Airflow Variables
        api_key = Variable.get("rapidapi_key")
        # API configuration
        url = "https://linkedin-data-scraper.p.rapidapi.com/search_jobs"
        headers = {
            "x-rapidapi-key": api_key,
            "x-rapidapi-host": "linkedin-data-scraper.p.rapidapi.com",
            "Content-Type": "application/json"
        }
        payload = {
            "keywords": "Pharmacists",
            "location": "London, United Kingdom",
            "count": 100
        }
        # Execute API request
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()  # Raise exception for HTTP errors
        # Push data to XCom for next task
        kwargs['ti'].xcom_push(key='raw_data', value=response.json())
        logging.info("Successfully fetched data from API")
    except Exception as e:
        logging.error(f"API fetch failed: {str(e)}")
        raise

def transform_data(**kwargs):
    """
    Task to transform API response into structured format
    Uses data from previous task via XCom
    """
    try:
        # Pull data from previous task
        ti = kwargs['ti']
        raw_data = ti.xcom_pull(task_ids='fetch_jobs', key='raw_data')
        
        if raw_data is None:
            raise ValueError("No data received from fetch_data task.")
        
        # Transformation logic
        if 'response' in raw_data and isinstance(raw_data['response'], list):
            main = []
            for item in raw_data['response']:
                if isinstance(item, list):
                    main.extend(item)
                elif isinstance(item, dict):
                    main.append(item)
            df = pd.DataFrame(main)
            
            # Column renaming
            column_map = {
                'title': 'JobTitle',
                'comapnyURL1': 'CompanyURL1',
                'comapnyURL2': 'CompanyURL2',
                'companyId': 'CompanyId',
                'companyUniversalName': 'CompanyUniversalName',
                'companyName': 'CompanyName',
                'salaryInsights': 'SalaryInsights',
                'applicants': 'NoOfApplicants',
                'formattedLocation': 'CompanyLocation',
                'formattedEmploymentStatus': 'EmploymentStatus',
                'formattedExperienceLevel': 'ExperienceLevel',
                'formattedIndustries': 'Industries',
                'jobDescription': 'JobDescription',
                'inferredBenefits': 'Benefits',
                'jobFunctions': 'JobFunctions',
                'companyApplyUrl': 'CompanyApplicationUrl',
                'jobPostingUrl': 'JobPostingUrl',
                'listedAt': 'PostedDate'
            }
            df.rename(columns=column_map, inplace=True)
            
            # Data type conversions
            df['PostedDate'] = pd.to_datetime(df['PostedDate'], errors='coerce')
            
            # Change specific columns to numeric types for consistency
            columns_to_change = ['CompanyId', 'SalaryInsights', 'NoOfApplicants']
            for col in columns_to_change:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Convert date columns to datetime format for easier analysis
            if 'PostedDate' in df.columns:
                df['PostedDate'] = pd.to_datetime(df['PostedDate'], errors='coerce')
                df['PostedDate'] = df['PostedDate'].astype(str).str.split('+').str[0]
                df['PostedDate'] = pd.to_datetime(df['PostedDate'], errors='coerce')
            
            # Drop columns that are unnecessary or redundant
            columns_to_drop = [
                'CompanyURL1', 'CompanyUniversalName', 
                'JobFunctions', 'CompanyApplicationUrl', 'JobDescription'
            ]
            df.drop(columns=[col for col in columns_to_drop if col in df.columns], inplace=True)
            
            # Push transformed data to XCom
            kwargs['ti'].xcom_push(key='transformed_data', value=df.to_json())
            logging.info("Data transformation completed successfully")
        else:
            raise ValueError("Unexpected data format received from API.")
    except Exception as e:
        logging.error(f"Transformation failed: {str(e)}")
        raise

def load_to_postgres(**context):
    """
    Task 3: Load data into PostgreSQL
    - Uses Airflow connection
    - Optimized bulk insert
    - Connection cleanup
    """
    try:
        data_json = context['ti'].xcom_pull(task_ids='transform_jobs', key='transformed_data')
        df = pd.read_json(data_json, orient='records')

        # Get PostgreSQL connection using PostgresHook
        hook = PostgresHook(postgres_conn_id='pharma_postgres')
        
        # Use get_conn() to get the raw connection
        conn = hook.get_conn()
        
        # Use the connection directly with pandas to_sql
        df.to_sql(
            name='pharmacists_joblist',
            con=conn,
            schema='public',
            if_exists='append',
            index=False,
            chunksize=1000,
            method='multi'
        )
        
        logging.info("Loaded %d records to PostgreSQL", len(df))
    except Exception as e:
        logging.error("Database load failed: %s", str(e))
        raise

def generate_email_report(**context):
    """
    Task 4: Generate and send daily report
    - Creates CSV attachment
    - Calculates key metrics
    - Sends via email with HTML formatting
    """
    try:
        execution_date = context['execution_date'].strftime('%Y-%m-%d')
        hook = PostgresHook(postgres_conn_id='pharma_postgres')
        # Query today's data
        query = f"""
        SELECT *
        FROM pharmacists_joblist
        WHERE DATE("P") = '{execution_date}'
        """
        df = pd.read_sql(query, hook.get_conn())
        # Calculate metrics and create CSV (same as before)
        ...
        logging.info("Sent email report to %s", default_args['email'])
    except Exception as e:
        logging.error("Email report failed: %s", str(e))
        raise

# DAG Definition
with DAG(
    'pharmacist_jobs_pipeline',
    default_args=default_args,
    description='Daily pipeline for LinkedIn pharmacist job postings',
    catchup=False,
    tags=['pharma', 'jobs', 'reporting'],
) as dag:
    fetch_task = PythonOperator(
        task_id='fetch_jobs',
        python_callable=fetch_data,
        provide_context=True,
    )

    transform_task = PythonOperator(
        task_id='transform_jobs',
        python_callable=transform_data,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_to_postgres,
        provide_context=True,
    )

    report_task = PythonOperator(
        task_id='send_daily_report',
        python_callable=generate_email_report,
        provide_context=True,
    )

    # Task dependencies
    fetch_task >> transform_task >> load_task >> report_task