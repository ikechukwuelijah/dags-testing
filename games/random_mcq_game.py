from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import psycopg2
from psycopg2 import extras

default_args = {
    'owner': 'Ik',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 11),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'daily_random_mcq_etl',
    default_args=default_args,
    description='Daily ETL for random MCQ game quiz',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

def extract_data(**kwargs):
    url = "https://game-quiz.p.rapidapi.com/quiz/random"
    querystring = {
        "lang": "en", "amount": "100", "format": "mcq",
        "cached": "false", "endpoint": "games"
    }
    headers = {
        "x-rapidapi-key": "e7cbee4ae1mshd0704b6dcc65548p1632e3jsncb04d537a049",
        "x-rapidapi-host": "game-quiz.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)
    json_data = response.json()

    kwargs['ti'].xcom_push(key='raw_data', value=json_data)

def transform_data(**kwargs):
    ti = kwargs['ti']
    json_data = ti.xcom_pull(task_ids='extract', key='raw_data')

    records = []
    for item in json_data.get("data", []):
        question = item.get("question")
        correct = item.get("options", {}).get("correct")
        incorrect = item.get("options", {}).get("incorrect", [])
        incorrect = (incorrect + [None] * 3)[:3]

        record = {
            "question": question,
            "correct_answer": correct,
            "option_1": correct,
            "option_2": incorrect[0],
            "option_3": incorrect[1],
            "option_4": incorrect[2],
            "reference": item.get("reference", [None])[0],
            "extra_type": item.get("extra", {}).get("type"),
            "extra_content": item.get("extra", {}).get("content"),
            "is_image": item.get("options", {}).get("is_image", False)
        }
        records.append(record)

    df = pd.DataFrame(records)
    ti.xcom_push(key='transformed_data', value=df.to_dict(orient='records'))

def load_data(**kwargs):
    ti = kwargs['ti']
    records = ti.xcom_pull(task_ids='transform', key='transformed_data')

    df = pd.DataFrame(records)

    db_params = {
        "dbname": "dwh",
        "user": "ikeengr",
        "password": "DataEngineer247",
        "host": "89.40.0.150",
        "port": "5432"
    }

    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    create_table_query = """
    CREATE TABLE IF NOT EXISTS random_mcq_game_quiz (
        id SERIAL PRIMARY KEY,
        question TEXT,
        correct_answer TEXT,
        option_1 TEXT,
        option_2 TEXT,
        option_3 TEXT,
        option_4 TEXT,
        reference TEXT,
        extra_type TEXT,
        extra_content TEXT,
        is_image BOOLEAN
    );
    """
    cur.execute(create_table_query)
    conn.commit()

    insert_query = """
    INSERT INTO random_mcq_game_quiz (
        question, correct_answer, option_1, option_2, 
        option_3, option_4, reference, extra_type, 
        extra_content, is_image
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    extras.execute_batch(
        cur,
        insert_query,
        df.to_records(index=False).tolist()
    )

    conn.commit()
    cur.close()
    conn.close()

# Task Definitions
extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

# Task Dependencies
extract_task >> transform_task >> load_task
