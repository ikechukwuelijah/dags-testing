from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import json
import pandas as pd

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
    'fetch_and_store_jokes',
    default_args=default_args,
    description='Fetch jokes from API and store them in PostgreSQL',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Function to fetch jokes
    def fetch_jokes():
        urls = {
            "general": "https://official-joke-api.appspot.com/jokes/general/ten",
            "programming": "https://official-joke-api.appspot.com/jokes/programming/ten"
        }
        all_jokes = []
        for url in urls.values():
            response = requests.get(url)
            if response.status_code == 200:
                jokes = response.json()
                print("JSON data from API:", json.dumps(jokes, indent=4))
                all_jokes.extend(jokes)
            else:
                print(f"Failed to retrieve data from {url}")
        
        # Save fetched jokes as a JSON file in the Airflow tmp folder
        with open('/tmp/jokes.json', 'w') as f:
            json.dump(all_jokes, f)
    
    # Function to transform data into DataFrame and return i
#%% step 3
t
    def transform_jokes():
        with open('/tmp/jokes.json', 'r') as f:
            all_jokes = json.load(f)
        
        df = pd.DataFrame(all_jokes)
        print(df)
        return df.to_dict('records')
    
    # Function to load data into PostgreSQL using PostgresHook
    def load_jokes(**kwargs):
        hook = PostgresHook(postgres_conn_id='postgres_dwh')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        create_table_query = """
        CREATE TABLE IF NOT EXISTS jokes (
            id SERIAL PRIMARY KEY,
            joke_id INTEGER UNIQUE,
            setup TEXT,
            punchline TEXT,
            category VARCHAR(50)
        );
        """
        cursor.execute(create_table_query)
        conn.commit()
        
        ti = kwargs['ti']
        jokes = ti.xcom_pull(task_ids='transform_jokes')
        
        insert_query = """
        INSERT INTO jokes (joke_id, setup, punchline, category)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (joke_id) DO NOTHING;
        """
        
        for joke in jokes:
            cursor.execute(insert_query, (joke['id'], joke['setup'], joke['punchline'], None))
        
        conn.commit()
        cursor.close()
        conn.close()
        print("Data successfully loaded into PostgreSQL database.")
    
    # Define the tasks
    fetch_task = PythonOperator(
        task_id='fetch_jokes',
        python_callable=fetch_jokes
    )
    
    transform_task = PythonOperator(
        task_id='transform_jokes',
        python_callable=transform_jokes,
        provide_context=True
    )
    
    load_task = PythonOperator(
        task_id='load_jokes',
        python_callable=load_jokes,
        provide_context=True
    )
    
    # Task dependencies
    fetch_task >> transform_task >> load_task
