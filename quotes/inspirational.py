from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import psycopg2

default_args = {
    'owner': 'Ikeengr',
    'start_date': datetime(2025, 4, 8),
    'retries': 1
}

dag = DAG(
    'inspirational_dag',
    description='Fetch and store inspirational quotes',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

# Task 1: Fetch quote from API
def fetch_quote(**kwargs):
    url = "https://quotes-api12.p.rapidapi.com/quotes/random"
    querystring = {"type": "inspirational"}
    headers = {
        "x-rapidapi-key": "efbc12a764msh39a81e663d3e104p1e76acjsn337fd1d56751",
        "x-rapidapi-host": "quotes-api12.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)
    quote_data = response.json()
    
    # Push to XCom
    kwargs['ti'].xcom_push(key='quote_data', value=quote_data)

# Task 2: Load quote into PostgreSQL
def load_to_postgres(**kwargs):
    # Pull from XCom
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='fetch_quote', key='quote_data')

    conn = psycopg2.connect(
        dbname="dwh",
        user="ikeengr",
        password="DataEngineer247",
        host="89.40.0.150",
        port="5432"
    )
    cur = conn.cursor()

    # Ensure table exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS inspirational (
            id SERIAL PRIMARY KEY,
            quote TEXT,
            author TEXT,
            type TEXT
        )
    """)

    # Insert the quote
    cur.execute("""
        INSERT INTO inspirational (quote, author, type)
        VALUES (%s, %s, %s)
    """, (data['quote'], data['author'], data['type']))

    conn.commit()
    cur.close()
    conn.close()

# Define tasks
fetch_task = PythonOperator(
    task_id='fetch_quote',
    python_callable=fetch_quote,
    provide_context=True,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_quote',
    python_callable=load_to_postgres,
    provide_context=True,
    dag=dag
)

# Set task dependencies
fetch_task >> load_task
