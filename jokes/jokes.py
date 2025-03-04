from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import requests
import pandas as pd

# Define default arguments for the DAG
default_args = {
    'owner': 'Ik',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 4),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'jokes_to_postgres',
    default_args=default_args,
    description='Fetch jokes and load them to PostgreSQL every hour',
    schedule_interval='@hourly',
)

def fetch_jokes():
    # Define the base URLs for the categories
    urls = {
        "general": "https://official-joke-api.appspot.com/jokes/general/ten",
        "programming": "https://official-joke-api.appspot.com/jokes/programming/ten"
    }

    # Fetch jokes from multiple categories
    all_jokes = []
    for category, url in urls.items():
        response = requests.get(url)
        if response.status_code == 200:
            jokes = response.json()
            for joke in jokes:
                joke['category'] = category  # Add a category label to each joke
            all_jokes.extend(jokes)
        else:
            print(f"Failed to retrieve data from {category} category")

    # Convert combined data into a DataFrame
    if all_jokes:
        df = pd.DataFrame(all_jokes)
        return df
    else:
        print("No jokes retrieved")
        return None

def load_to_postgres(df):
    if df is not None:
        # Initialize PostgresHook
        postgres_hook = PostgresHook(postgres_conn_id='postgres_dwh')

        # Create the table if it doesn't exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS jokes (
            id SERIAL PRIMARY KEY,
            joke_id INTEGER UNIQUE,
            setup TEXT,
            punchline TEXT,
            category VARCHAR(50)
        );
        """
        postgres_hook.run(create_table_query)

        # Insert jokes into the table
        insert_query = """
        INSERT INTO jokes (joke_id, setup, punchline, category)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (joke_id) DO NOTHING;
        """
        rows = [tuple(x) for x in df[['id', 'setup', 'punchline', 'category']].values]
        postgres_hook.insert_rows(table='jokes', rows=rows, target_fields=['joke_id', 'setup', 'punchline', 'category'], commit_every=1000)

        print("Data successfully loaded into PostgreSQL database.")
    else:
        print("No data to load")

# Define the tasks
fetch_task = PythonOperator(
    task_id='fetch_jokes',
    python_callable=fetch_jokes,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=lambda: load_to_postgres(fetch_task.output),
    dag=dag,
)

# Set the task dependencies
fetch_task >> load_task
