from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import pandas as pd

# Define default arguments for the DAG
default_args = {
    'owner': 'Ik',
    'start_date': datetime(2025, 3, 1),  # Start date for the DAG
    'retries': 1,  # Number of retry attempts if a task fails
    'retry_delay': timedelta(minutes=5),  # Wait time before retrying a failed task
}

# Define the Airflow DAG
dag = DAG(
    'uk_police_crime_data',
    default_args=default_args,
    schedule_interval='@weekly',  # Runs every week
    catchup=False  # Prevents running missed historical DAGs
)

# Function to fetch crime data from the UK Police API
def fetch_data(**kwargs):
    url = "https://data.police.uk/api/crimes-street/all-crime"
    params = {
        'lat': 52.629729,  # Latitude for Leicester, UK
        'lng': -1.131592,  # Longitude for Leicester, UK
        'date': '2023-01'  # Fetching data for January 2023
    }

    response = requests.get(url, params=params)

    # Check if request was successful
    if response.status_code == 200:
        data = response.json()
        kwargs['ti'].xcom_push(key='raw_data', value=data)  # Store data in XCom
    else:
        raise ValueError(f"Failed to fetch data, status code: {response.status_code}")

# Function to transform data into a Pandas DataFrame
def transform_data(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(key='raw_data', task_ids='fetch_data')

    if data:
        df = pd.json_normalize(data)  # Convert JSON to DataFrame
        ti.xcom_push(key='transformed_data', value=df.to_json())  # Store DataFrame as JSON in XCom
    else:
        raise ValueError("No data found to transform.")

# Function to upload data to PostgreSQL using PostgresHook
def upload_to_postgres(**kwargs):
    ti = kwargs['ti']
    df_json = ti.xcom_pull(key='transformed_data', task_ids='transform_data')

    if df_json:
        df = pd.read_json(df_json)  # Convert JSON back to DataFrame

        try:
            # Establish connection to PostgreSQL using Airflow's PostgresHook
            hook = PostgresHook(postgres_conn_id='postgres_dwh')
            conn = hook.get_conn()
            cursor = conn.cursor()

            # Create table if it doesn't exist
            create_table_query = """
            CREATE TABLE IF NOT EXISTS uk_police_crime (
                id SERIAL PRIMARY KEY,
                category TEXT,
                location_type TEXT,
                latitude FLOAT,
                longitude FLOAT,
                street_name TEXT,
                context TEXT,
                outcome_status TEXT,
                persistent_id TEXT,
                crime_id TEXT UNIQUE
            );
            """
            cursor.execute(create_table_query)
            conn.commit()

            # Insert data into PostgreSQL
            for _, row in df.iterrows():
                insert_query = """
                INSERT INTO uk_police_crime (category, location_type, latitude, longitude, street_name, context, 
                                             outcome_status, persistent_id, crime_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (crime_id) DO NOTHING;
                """
                cursor.execute(insert_query, (
                    row.get('category'),
                    row.get('location_type'),
                    row.get('location.latitude'),
                    row.get('location.longitude'),
                    row.get('location.street.name'),
                    row.get('context'),
                    row.get('outcome_status.category') if row.get('outcome_status') else None,
                    row.get('persistent_id'),
                    row.get('id')
                ))

            conn.commit()
            cursor.close()
            conn.close()
            print("Data uploaded successfully to PostgreSQL.")

        except Exception as e:
            raise ValueError(f"Error uploading data to PostgreSQL: {e}")

# Define Airflow tasks
fetch_task = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_data,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

upload_task = PythonOperator(
    task_id='upload_to_postgres',
    python_callable=upload_to_postgres,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
fetch_task >> transform_task >> upload_task
