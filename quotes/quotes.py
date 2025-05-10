from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import pandas as pd
import json

# Default DAG arguments
default_args = {
    'owner': 'Ik',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'fetch_and_store_quotes',
    default_args=default_args,
    description='Fetch quotes from API and store them in PostgreSQL',
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    # Function to fetch quotes from API
    def fetch_quotes():
        url = "https://quotes15.p.rapidapi.com/quotes/random/"
        headers = {
            "x-rapidapi-key": "7b66ced988msh253ab4a526f3148p1eed78jsn4d8bcaa48242",  # Secure API key storage recommended
            "x-rapidapi-host": "quotes15.p.rapidapi.com"
        }
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            with open('/tmp/quotes.json', 'w') as f:
                json.dump(data, f)
        else:
            raise Exception(f"Failed to fetch data: {response.status_code}")
    
    # Function to transform JSON data into DataFrame format
    def transform_quotes():
        with open('/tmp/quotes.json', 'r') as f:
            data = json.load(f)
        
        df = pd.DataFrame([{
            'quote_id': data['id'],
            'quote_content': data['content'],
            'quote_url': data['url'],
            'quote_language': data['language_code'],
            'originator_id': data['originator']['id'],
            'originator_name': data['originator']['name'],
            'originator_url': data['originator']['url'],
            'tags': ', '.join(data['tags'])  # Convert list to string
        }])
        
        return df.to_dict('records')
    
    # Function to load transformed data into PostgreSQL

   def load_quotes(**kwargs):

    hook = PostgresHook(postgres_conn_id='postgres_dwh')
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Optional: Drop table if it exists (for dev purposes)
    # cursor.execute("DROP TABLE IF EXISTS quotes")

    create_table_query = """
    CREATE TABLE IF NOT EXISTS quotes (
        id SERIAL PRIMARY KEY,
        quote_id INTEGER UNIQUE,
        quote_content TEXT,
        quote_url TEXT,
        quote_language VARCHAR(10),
        originator_id INTEGER,
        originator_name VARCHAR(100),
        originator_url TEXT,
        tags TEXT
    );
    """
    cursor.execute(create_table_query)

    # Ensure schema is up-to-date (add missing column if needed)
    cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name='quotes';")
    existing_columns = [row[0] for row in cursor.fetchall()]
    if 'quote_id' not in existing_columns:
        cursor.execute("ALTER TABLE quotes ADD COLUMN quote_id INTEGER UNIQUE;")
        conn.commit()

    ti = kwargs['ti']
    quotes = ti.xcom_pull(task_ids='transform_quotes')

    insert_query = """
    INSERT INTO quotes (quote_id, quote_content, quote_url, quote_language, originator_id, originator_name, originator_url, tags)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (quote_id) DO NOTHING;
    """

    for quote in quotes:
        cursor.execute(insert_query, (
            quote['quote_id'],
            quote['quote_content'],
            quote['quote_url'],
            quote['quote_language'],
            quote['originator_id'],
            quote['originator_name'],
            quote['originator_url'],
            quote['tags']
        ))

    conn.commit()
    cursor.close()
    conn.close()

