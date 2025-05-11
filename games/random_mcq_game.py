import requests
import pandas as pd
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
import os

# ----------------------------
# Function to fetch data from API
# ----------------------------
def extract_data(**kwargs):
    url = "https://game-quiz.p.rapidapi.com/quiz/random"
    querystring = {"lang": "en", "amount": "100", "format": "mcq", "cached": "false", "endpoint": "games"}

    headers = {
        "x-rapidapi-key": "e7cbee4ae1mshd0704b6dcc65548p1632e3jsncb04d537a049",
        "x-rapidapi-host": "game-quiz.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)
    response_data = response.json()

    # Push the extracted response data to XCom
    ti = kwargs['ti']
    ti.xcom_push(key='response_data', value=response_data)

# ---------------------------
# Function to transform data
# ---------------------------
def transform_data(**kwargs):
    ti = kwargs['ti']
    response_data = ti.xcom_pull(task_ids='extract_data', key='response_data')

    if response_data:
        # Flatten JSON data into a pandas DataFrame
        df = pd.json_normalize(response_data)
        
        # Transform the data as needed
        # (Modify the column names to fit your needs)
        df_transformed = df[['question', 'correct_answer', 'options', 'reference', 'type', 'extra', 'is_image']]

        # Create a new column for each option
        df_transformed[['option_1', 'option_2', 'option_3', 'option_4']] = pd.DataFrame(df['options'].tolist(), index=df.index)

        # Save the transformed data to a CSV file
        temp_file_path = "/tmp/transformed_data.csv"
        df_transformed.to_csv(temp_file_path, index=False)

        # Push the file path to XCom for the next task
        ti.xcom_push(key='transformed_file_path', value=temp_file_path)
    else:
        raise ValueError("No data extracted from the API!")

# --------------------------
# Function to load data to Postgres
# --------------------------
def load_data_to_postgres(**kwargs):
    ti = kwargs['ti']
    file_path = ti.xcom_pull(task_ids='transform_data', key='transformed_file_path')

    if file_path:
        # Read the transformed data from CSV file
        df_transformed = pd.read_csv(file_path)

        # Setup PostgreSQL connection
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Create the table if it does not exist
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
        cursor.execute(create_table_query)
        conn.commit()

        # Insert data into the PostgreSQL table
        for _, row in df_transformed.iterrows():
            insert_query = """
            INSERT INTO random_mcq_game_quiz (
                question, correct_answer, option_1, option_2, option_3, option_4,
                reference, extra_type, extra_content, is_image
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, tuple(row))

        # Commit the transaction
        conn.commit()

        # Close the cursor and connection
        cursor.close()
        conn.close()
    else:
        print("No transformed data file to load into PostgreSQL.")

# -------------------------
# Airflow DAG
# -------------------------
with DAG(
    'random_mcq_game_quiz',
    default_args={
        'owner': 'Ik',
        'retries': 1,
    },
    description='ETL pipeline for fetching random MCQ game quiz data',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Task 1: Extract data from API
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        provide_context=True,
    )

    # Task 2: Transform the data
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True,
    )

    # Task 3: Load data to PostgreSQL
    load_task = PythonOperator(
        task_id='load_data_to_postgres',
        python_callable=load_data_to_postgres,
        provide_context=True,
    )

    # Set task dependencies
    extract_task >> transform_task >> load_task
