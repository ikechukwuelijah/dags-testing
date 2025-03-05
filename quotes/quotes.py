from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import requests
import pandas as pd
import time

# Default arguments for DAG
default_args = {
    'owner': 'IK',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 4),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    'quotes_to_postgres',
    default_args=default_args,
    description='Fetch quotes and load them to PostgreSQL every hour',
    schedule_interval='@hourly',
)

def fetch_quotes(**kwargs):
    url = "https://quotes15.p.rapidapi.com/quotes/random/"
    querystring = {"language_code": "en"}
    headers = {
        "x-rapidapi-key": "7b66ced988msh253ab4a526f3148p1eed78jsn4d8bcaa48242",
        "x-rapidapi-host": "quotes15.p.rapidapi.com"
    }

    all_quotes = []
    for _ in range(10):  # Fetch 10 quotes
        try:
            response = requests.get(url, headers=headers, params=querystring)
            response.raise_for_status()

            if response.status_code == 429:
                print("Rate limit exceeded. Waiting...")
                time.sleep(2)

            data = response.json()
            quote_data = {
                "id": data.get("id") or data.get("quoteId"),
                "content": data.get("content"),
                "author": data.get("originator", {}).get("name", "Unknown"),
                "tags": ", ".join(data.get("tags", []))
            }
            all_quotes.append(quote_data)

            time.sleep(2)  # Avoid hitting rate limits

        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            continue

    # Push result to XCom
    kwargs['ti'].xcom_push(key='quotes_data', value=all_quotes)

def transform_data(**kwargs):
    ti = kwargs['ti']
    quote_data = ti.xcom_pull(task_ids='fetch_quotes', key='quotes_data')

    if not quote_data:
        print("No data received from fetch_quotes")
        return None
    
    # Convert to DataFrame
    df = pd.DataFrame(quote_data)
    return df.to_dict(orient='records')  # Convert to JSON-serializable format for XCom

def load_data_to_postgres(**kwargs):
    ti = kwargs['ti']
    quotes_data = ti.xcom_pull(task_ids='transform_data')

    if not quotes_data:
        print("No data to load into PostgreSQL")
        return

    df = pd.DataFrame(quotes_data)

    # Initialize PostgresHook
    postgres_hook = PostgresHook(postgres_conn_id='postgres_dwh')

    # Ensure the 'quotes' table exists
    create_table_query = """
    CREATE TABLE IF NOT EXISTS quotes (
        id BIGINT PRIMARY KEY,
        content TEXT NOT NULL,
        author TEXT,
        tags TEXT
    );
    """
    postgres_hook.run(create_table_query)

    # Insert data
    insert_query = """
    INSERT INTO quotes (id, content, author, tags)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (id) DO NOTHING;
    """
    rows = [tuple(x) for x in df[['id', 'content', 'author', 'tags']].values]
    postgres_hook.insert_rows(table='quotes', rows=rows, target_fields=['id', 'content', 'author', 'tags'], commit_every=1000)

fetch_task = PythonOperator(
    task_id='fetch_quotes',
    python_callable=fetch_quotes,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data_to_postgres',
    python_callable=load_data_to_postgres,
    provide_context=True,
    dag=dag,
)

fetch_task >> transform_task >> load_task
