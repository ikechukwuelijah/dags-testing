from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import time

# Default arguments for the DAG
default_args = {
    "owner": "Ik",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 26),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "fbi_wanted_list",
    default_args=default_args,
    description="Extract FBI wanted list and load into PostgreSQL",
    schedule_interval=timedelta(days=1),
    catchup=False,
)

API_URL = "https://api.fbi.gov/wanted/v1/list"

def get_total_records():
    """Fetches the total number of records available in the FBI Wanted API."""
    response = requests.get(API_URL)
    if response.status_code == 200:
        return response.json().get("total", 0)
    else:
        raise Exception(f"Failed to fetch total records. Status Code: {response.status_code}")

def fetch_all_records(ti):
    """Fetches all records from the FBI API using pagination and pushes to XCom."""
    all_records = []
    page = 1
    page_size = 50

    total_records = get_total_records()
    print(f"Total Records Available: {total_records}")

    while len(all_records) < total_records:
        print(f"Fetching page {page}...")
        response = requests.get(f"{API_URL}?pageSize={page_size}&page={page}")

        if response.status_code != 200:
            print(f"Error fetching page {page}. Status: {response.status_code}")
            break

        data = response.json().get("items", [])
        all_records.extend(data)

        if not data:
            break

        page += 1
        time.sleep(1)  # Avoid API rate limits

    print(f"Total Records Fetched: {len(all_records)}")
    ti.xcom_push(key="fbi_wanted_data", value=all_records)

def transform_data(ti):
    """Transforms raw JSON data into a structured Pandas DataFrame and pushes it to XCom."""
    data = ti.xcom_pull(task_ids="fetch_all_records", key="fbi_wanted_data")

    df = pd.DataFrame(data)
    columns_needed = ["title", "sex", "race", "dates_of_birth_used", "nationality", "reward_text", "url"]
    df = df[columns_needed]
    df.fillna("Unknown", inplace=True)

    transformed_data = df.to_dict(orient="records")
    ti.xcom_push(key="transformed_data", value=transformed_data)

def load_to_postgres(ti):
    """Loads the transformed data into PostgreSQL table `fbi_list` in `dwh` database."""
    records = ti.xcom_pull(task_ids="transform_data", key="transformed_data")

    if not records:
        raise ValueError("No data to insert into PostgreSQL")

    pg_hook = PostgresHook(postgres_conn_id="postgres_dwh")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    for record in records:
        try:
            cursor.execute(
                """
                INSERT INTO fbi_list (title, sex, race, dates_of_birth_used, nationality, reward_text, url)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (url) DO NOTHING;
                """,
                (record["title"], record["sex"], record["race"], record["dates_of_birth_used"],
                 record["nationality"], record["reward_text"], record["url"])
            )
        except Exception as e:
            print(f"Error inserting row: {e}")
            conn.rollback()
        else:
            conn.commit()

    cursor.close()
    conn.close()
    print(f"Successfully inserted {len(records)} records into PostgreSQL")

# Define the Airflow tasks
fetch_task = PythonOperator(
    task_id="fetch_all_records",
    python_callable=fetch_all_records,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform_data",
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id="load_to_postgres",
    python_callable=load_to_postgres,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
fetch_task >> transform_task >> load_task
