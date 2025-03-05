from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import requests
import pandas as pd

# Default DAG arguments
default_args = {
    'owner': 'Ik',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 4),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'jokes_to_postgres',
    default_args=default_args,
    description='Fetch jokes and load them to PostgreSQL every hour',
    schedule_interval='@hourly',
)

def fetch_jokes(**kwargs):
    urls = {
        "general": "https://official-joke-api.appspot.com/jokes/general/ten",
        "programming": "https://official-joke-api.appspot.com/jokes/programming/ten"
    }
    
    all_jokes = []
    for category, url in urls.items():
        response = requests.get(url)
        if response.status_code == 200:
            jokes = response.json()
            for joke in jokes:
                joke['category'] = category
            all_jokes.extend(jokes)
        else:
            print(f"Failed to retrieve jokes from {category} category")

    if all_jokes:
        # Push the jokes as a JSON-serializable list
        kwargs['ti'].xcom_push(key='jokes_data', value=all_jokes)
    else:
        print("No jokes retrieved")

def load_to_postgres(**kwargs):
    # Pull jokes from XCom
    ti = kwargs['ti']
    jokes_data = ti.xcom_pull(task_ids='fetch_jokes', key='jokes_data')

    if jokes_data:
        df = pd.DataFrame(jokes_data)

        # Initialize PostgresHook
        postgres_hook = PostgresHook(postgres_conn_id='postgres_dwh')

        # Create table if it doesn't exist
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

        # Insert data
        insert_query = """
        INSERT INTO jokes (joke_id, setup, punchline, category)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (joke_id) DO NOTHING;
        """
        rows = [tuple(x) for x in df[['id', 'setup', 'punchline', 'category']].values]
        postgres_hook.insert_rows(table='jokes', rows=rows, target_fields=['joke_id', 'setup', 'punchline', 'category'], commit_every=1000)

        print("Data successfully loaded into PostgreSQL.")
    else:
        print("No data to load")

fetch_task = PythonOperator(
    task_id='fetch_jokes',
    python_callable=fetch_jokes,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    provide_context=True,
    dag=dag,
)

fetch_task >> load_task
