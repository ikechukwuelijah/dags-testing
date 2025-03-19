from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import json

# API Configuration
API_ENDPOINT = "https://the-weed-db.p.rapidapi.com/api/strains"
API_HEADERS = {
    "x-rapidapi-key": "f38eae887bmsh5211e33c97c1c50p125cafjsnec52eb060a05",
    "x-rapidapi-host": "the-weed-db.p.rapidapi.com"
}

# Define default_args for Airflow DAG
default_args = {
    "owner": "Ik",
    "depends_on_past": False,
    "start_date": days_ago(1),  # Adjust for production
    "retries": 1,
}

# Define DAG
dag = DAG(
    "easy_grow_dag",
    default_args=default_args,
    description="Fetch easy-grow strains from API and load into PostgreSQL",
    schedule_interval="0 0 1 * *",  # Runs on the first of every month
    catchup=False,
)

# 🟢 Step 1: Extract Data from API
def fetch_data(**kwargs):
    import requests
    response = requests.get(API_ENDPOINT, headers=API_HEADERS, params={"growDifficulty": "easy"})
    if response.status_code == 200:
        data = response.json()
        kwargs["ti"].xcom_push(key="strain_data", value=data)  # Store in XCom
    else:
        raise Exception(f"API Request Failed: {response.status_code}")

extract_task = PythonOperator(
    task_id="extract_data",
    python_callable=fetch_data,
    provide_context=True,
    dag=dag,
)

# 🟡 Step 2: Transform Data
def transform_data(**kwargs):
    ti = kwargs["ti"]
    data = ti.xcom_pull(task_ids="extract_data", key="strain_data")
    
    if not data:
        raise ValueError("No data pulled from XCom")
    
    df = pd.DataFrame(data)
    
    # Convert to list of tuples for SQL insertion
    records = [
        (
            row.get("_id"), row.get("name"), row.get("link"), row.get("imageUrl"),
            row.get("description"), row.get("genetics"), row.get("THC"), row.get("CBD"),
            row.get("parents"), row.get("smellAndFlavour"), row.get("effect"), 
            row.get("growEnvironments"), row.get("growDifficulty"), row.get("floweringType"),
            row.get("floweringTime"), row.get("harvestTimeOutdoor"), row.get("yieldIndoor"),
            row.get("yieldOutdoor"), row.get("heightIndoor"), row.get("heightOutdoor")
        ) 
        for _, row in df.iterrows()
    ]

    ti.xcom_push(key="strain_records", value=records)

transform_task = PythonOperator(
    task_id="transform_data",
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

# 🔵 Step 3: Load Data to PostgreSQL
def load_to_postgres(**kwargs):
    ti = kwargs["ti"]
    records = ti.xcom_pull(task_ids="transform_data", key="strain_records")

    if not records:
        raise ValueError("No records found for insertion")

    # Initialize Postgres Hook
    postgres_hook = PostgresHook(postgres_conn_id="postgres_dwh")  # Ensure this is set up in Airflow UI
    
    # Create Table
    create_table_query = """
    CREATE TABLE IF NOT EXISTS easy_grow_strains (
        id TEXT PRIMARY KEY,
        name TEXT,
        link TEXT,
        imageUrl TEXT,
        description TEXT,
        genetics TEXT,
        THC TEXT,
        CBD TEXT,
        parents TEXT,
        smellAndFlavour TEXT,
        effect TEXT,
        growEnvironments TEXT,
        growDifficulty TEXT,
        floweringType TEXT,
        floweringTime TEXT,
        harvestTimeOutdoor TEXT,
        yieldIndoor TEXT,
        yieldOutdoor TEXT,
        heightIndoor TEXT,
        heightOutdoor TEXT
    );
    """
    
    # Insert Query
    insert_query = """
    INSERT INTO easy_grow_strains (
        id, name, link, imageUrl, description, genetics, THC, CBD, 
        parents, smellAndFlavour, effect, growEnvironments, growDifficulty, 
        floweringType, floweringTime, harvestTimeOutdoor, yieldIndoor, 
        yieldOutdoor, heightIndoor, heightOutdoor
    ) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (id) DO NOTHING;
    """
    
    # Execute Queries
    conn = postgres_hook.get_conn()
    cur = conn.cursor()
    
    try:
        cur.execute(create_table_query)
        cur.executemany(insert_query, records)  # Batch insert
        conn.commit()
        print("Data inserted successfully into PostgreSQL")
    except Exception as e:
        conn.rollback()
        raise Exception(f"Database Insertion Failed: {str(e)}")
    finally:
        cur.close()
        conn.close()

load_task = PythonOperator(
    task_id="load_to_postgres",
    python_callable=load_to_postgres,
    provide_context=True,
    dag=dag,
)

# Define Task Dependencies
extract_task >> transform_task >> load_task
