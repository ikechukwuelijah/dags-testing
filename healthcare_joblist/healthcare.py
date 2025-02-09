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
    'start_date': datetime(2025, 2, 9),  # Start date for the DAG
    'email': Variable.get("email_recipients"),  # Email recipients
    'email_on_failure': True,  # Send email on failure
    'email_on_retry': False,  # Do not send email on retry
    'retries': 2,  # Number of retries
    'retry_delay': timedelta(minutes=5),  # Retry delay
    'max_active_runs': 1  # Maximum active runs
}

# Instantiate the DAG
dag = DAG(
    'healthcare_job_extraction_pipeline',  # DAG ID
    default_args=default_args,
    description='ETL pipeline for Healthcare job postings from LinkedIn',
    schedule_interval='@daily',  # Runs once per day
    catchup=False,  # Do not backfill past runs
    tags=['healthcare', 'linkedin']  # Tags for categorization
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
        url = "https://linkedin-jobs-api2.p.rapidapi.com/active-jb-24h"
        querystring = {
            "title_filter": "\"Healthcare Assistant\"",  # Filter for specific job titles
            "location_filter": "\"United Kingdom\"",
            "count": "50"  # Limit the number of results to 50
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


def transform_data(**kwargs):
    """
    Task to transform API response into structured format.
    Pulls raw data from XCom and pushes transformed data to XCom.
    """
    try:
        # Pull raw data from XCom
        ti = kwargs['ti']
        raw_data = ti.xcom_pull(task_ids='fetch_data', key='raw_data')

        # Flatten the nested structure and create a DataFrame
        flattened_data = []
        for item in raw_data:
            record = {
                "id": item.get("id"),
                "date_posted": item.get("date_posted"),
                "title": item.get("title"),
                "organization": item.get("organization"),
                "organization_url": item.get("organization_url"),
                "date_validthrough": item.get("date_validthrough"),
                "location_country": item["locations_raw"][0]["address"]["addressCountry"] if item.get("locations_raw") else None,
                "location_locality": item["locations_raw"][0]["address"]["addressLocality"] if item.get("locations_raw") else None,
                "latitude": item["locations_raw"][0].get("latitude") if item.get("locations_raw") else None,
                "longitude": item["locations_raw"][0].get("longitude") if item.get("locations_raw") else None,
                "employment_type": ", ".join(item.get("employment_type", [])),
                "url": item.get("url"),
                "linkedin_org_employees": item.get("linkedin_org_employees"),
                "linkedin_org_size": item.get("linkedin_org_size"),
                "linkedin_org_industry": item.get("linkedin_org_industry"),
                "linkedin_org_locations": ", ".join(item.get("linkedin_org_locations", [])),
                "seniority": item.get("seniority")
            }
            flattened_data.append(record)

        # Convert to DataFrame
        df = pd.DataFrame(flattened_data)

        # Push transformed data to XCom as JSON
        kwargs['ti'].xcom_push(key='transformed_data', value=df.to_json(orient='records'))
        print(f"Transformed {len(df)} records.")
    except Exception as e:
        print(f"Error transforming data: {str(e)}")
        raise
    
def load_to_postgres(**kwargs):
    """
    Load transformed data into PostgreSQL using PostgresHook.
    Pulls transformed data from XCom and uses bulk insert.
    """
    try:
        # Pull transformed data from XCom
        ti = kwargs['ti']
        transformed_data = ti.xcom_pull(task_ids='transform_data', key='transformed_data')
        df = pd.read_json(transformed_data, orient='records')

        # Initialize PostgresHook with Airflow connection
        pg_hook = PostgresHook(postgres_conn_id="postgres_dwh")
        
        # Get SQLAlchemy engine from the hook
        engine = pg_hook.get_sqlalchemy_engine()

        # Get a raw DBAPI connection (doesn't support context manager)
        conn = engine.raw_connection()
        try:
            # Load data into PostgreSQL
            df.to_sql(
                name="healthcare_joblist",
                con=conn,
                schema="public",
                if_exists="append",
                index=False
            )
            # Commit the transaction
            conn.commit()
        finally:
            # Ensure the connection is closed
            conn.close()

        print(f"Loaded {len(df)} records into PostgreSQL.")
    except Exception as e:
        print(f"Error loading data into PostgreSQL: {str(e)}")
        raise


def send_email_report(**kwargs):
    """
    Task to send a daily email report.
    Includes key metrics and attaches a CSV file of the fetched data.
    """
    try:
        # Pull transformed data from XCom
        ti = kwargs['ti']
        transformed_data = ti.xcom_pull(task_ids='transform_data', key='transformed_data')
        df = pd.read_json(transformed_data, orient='records')

        # Calculate key metrics
        total_jobs = len(df)
        top_companies = df['organization'].value_counts().head(3).to_dict()
        common_locations = df['location_locality'].value_counts().head(3).to_dict()
        avg_seniority = df['seniority'].mode()[0] if not df.empty else 'N/A'

        # Create CSV attachment
        csv_buffer = df.to_csv(index=False)

        # Email content
        subject = f"Daily Healthcare Job Report - {datetime.now().strftime('%Y-%m-%d')}"
        html_content = f"""
        <h3>Daily Jobs Report</h3>
        <p>Total Jobs Collected: {total_jobs}</p>
        <p>Top Hiring Companies:</p>
        <ul>
            {"".join(f"<li>{k}: {v} jobs</li>" for k, v in top_companies.items())}
        </ul>
        <p>Common Locations:</p>
        <ul>
            {"".join(f"<li>{k}: {v} postings</li>" for k, v in common_locations.items())}
        </ul>
        <p>Average Seniority Level: {avg_seniority}</p>
        """

        # Send email
        send_email(
            to=default_args['email'],
            subject=subject,
            html_content=html_content,
            files=[{
                'filename': f'healthcare_jobs_{datetime.now().strftime("%Y-%m-%d")}.csv',
                'content': csv_buffer
            }]
        )
        print("Email report sent successfully.")
    except Exception as e:
        print(f"Error sending email report: {str(e)}")
        raise

# ====================================================
# 3. DEFINE DAG TASKS
# ====================================================

with dag:
    # Task 1: Fetch data from LinkedIn API
    fetch_task = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data,
        provide_context=True
    )

    # Task 2: Transform the fetched data
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True
    )

    # Task 3: Load transformed data into PostgreSQL
    load_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
        provide_context=True
    )

    # Task 4: Send daily email report
    email_task = PythonOperator(
        task_id='send_email_report',
        python_callable=send_email_report,
        provide_context=True
    )

    # Define task dependencies
    fetch_task >> transform_task >> load_task >> email_task
