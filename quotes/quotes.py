from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import requests
import pandas as pd
import time

# Define default arguments for the DAG
default_args = {
    'owner': 'IK',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 4),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'quotes_to_postgres',
    default_args=default_args,
    description='Fetch quotes and load them to PostgreSQL every hour',
    schedule_interval='@hourly',
)

def fetch_quotes(num_quotes=10, delay_seconds=2):
    url = "https://quotes15.p.rapidapi.com/quotes/random/"
    querystring = {"language_code": "en"}
    headers = {
        "x-rapidapi-key": "7b66ced988msh253ab4a526f3148p1eed78jsn4d8bcaa48242",
        "x-rapidapi-host": "quotes15.p.rapidapi.com"
    }

    all_quotes = []
    
    for _ in range(num_quotes):
        try:
            response = requests.get(url, headers=headers, params=querystring)
            response.raise_for_status()  # Raise an error for bad responses (4xx, 5xx)
            
            # Check for 429 status code (Too Many Requests)
            if response.status_code == 429:
                print("Rate limit exceeded. Waiting before retrying...")
                time.sleep(delay_seconds)  # Delay for a specified time to avoid rate limiting

            data = response.json()
            print("API Response:", data)  # Debugging: Print the raw response

            # Extract relevant fields
            quote_data = {
                "id": data.get("id") or data.get("quoteId"),  # Ensure correct ID extraction
                "content": data.get("content"),
                "author": data.get("originator", {}).get("name", "Unknown"),
                "tags": ", ".join(data.get("tags", []))  # Convert list to string
            }
            all_quotes.append(quote_data)

            # Wait for a brief period to avoid hitting rate limits
            time.sleep(delay_seconds)  # Add delay after each request to prevent hitting the rate limit

        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            continue  # Skip this request and move to the next one

    return all_quotes

def transform_data(quote_data):
    if not quote_data:
        return None  # No data fetched
    return pd.DataFrame(quote_data)

def load_data_to_postgres(df):
    if df is not None and not df.empty:
        postgres_hook = PostgresHook(postgres_conn_id='postgres_dwh')

        # Ensure the 'quotes' table exists with the required 'tags' column
        create_table_query = """
        CREATE TABLE IF NOT EXISTS quotes (
            id BIGINT PRIMARY KEY,
            content TEXT NOT NULL,
            author TEXT,
            tags TEXT
        );
        """
        postgres_hook.run(create_table_query)

        # Alter table to add 'tags' column if it doesn't exist (for safety)
        alter_table_query = """
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'quotes' AND column_name = 'tags') THEN
                ALTER TABLE quotes ADD COLUMN tags TEXT;
            END IF;
        END $$;
        """
        postgres_hook.run(alter_table_query)

        # Insert data using ON CONFLICT (Prevents duplicate entries)
        insert_query = """
        INSERT INTO quotes (id, content, author, tags)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING;
        """
        rows = [tuple(x) for x in df[['id', 'content', 'author', 'tags']].values]
        postgres_hook.insert_rows(table='quotes', rows=rows, target_fields=['id', 'content', 'author', 'tags'], commit_every=1000)

# Define the tasks
fetch_task = PythonOperator(
    task_id='fetch_quotes',
    python_callable=fetch_quotes,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    op_kwargs={'quote_data': '{{ task_instance.xcom_pull(task_ids="fetch_quotes") }}'},
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data_to_postgres',
    python_callable=load_data_to_postgres,
    op_kwargs={'df': '{{ task_instance.xcom_pull(task_ids="transform_data") }}'},
    dag=dag,
)

# Set the task dependencies
fetch_task >> transform_task >> load_task
