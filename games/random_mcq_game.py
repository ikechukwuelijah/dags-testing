from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
import requests
import pandas as pd
import psycopg2
from datetime import datetime, timedelta

# -------------------------------
# Define the DAG arguments
# -------------------------------
default_args = {
    'owner': 'Ik',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 11),  # Adjust start date accordingly
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# -------------------------------
# Define the DAG
# -------------------------------
dag = DAG(
    'random_mcq_game_quiz',  # DAG Name
    default_args=default_args,
    description='ETL for random MCQ game quiz',
    schedule_interval=timedelta(days=1),  # Run daily
    catchup=False,  # Do not backfill
)

# -------------------------------
# Step 1: Extract - Fetch API Data
# -------------------------------

def extract_data():
    url = "https://game-quiz.p.rapidapi.com/quiz/random"
    querystring = {"lang":"en","amount":"100","format":"mcq","cached":"false","endpoint":"games"}
    headers = {
        "x-rapidapi-key": "e7cbee4ae1mshd0704b6dcc65548p1632e3jsncb04d537a049",
        "x-rapidapi-host": "game-quiz.p.rapidapi.com"
    }
    
    response = requests.get(url, headers=headers, params=querystring)
    response_data = response.json()
    
    print("Extracted API Response:", response_data)  # Debugging log

    if response_data['status'] == 'ok':
        # Push data to XCom
        return response_data['data']
    else:
        print("API response status not OK:", response_data)  # Debugging log
        return None

# -------------------------------
# Step 2: Transform - Convert API response to DataFrame
# -------------------------------

def transform_data(**kwargs):
    # Get data from XCom
    ti = kwargs['ti']
    response_data = ti.xcom_pull(task_ids='extract_data')

    print("Data pulled from XCom:", response_data)  # Debugging log

    if response_data:
        # Convert to DataFrame
        df = pd.json_normalize(response_data)
        
        print("Normalized DataFrame:", df.head())  # Debugging log

        # Select required columns (adjust if needed)
        try:
            df_transformed = df[['question', 'options.correct', 'options.incorrect', 'reference', 'extra.type', 'extra.content', 'options.is_image']]
            df_transformed.columns = ['question', 'correct_answer', 'option_1', 'option_2', 'option_3', 'option_4', 'reference', 'extra_type', 'extra_content', 'is_image']
            
            print("Transformed DataFrame:", df_transformed.head())  # Debugging log

            # Push the DataFrame to XCom
            ti.xcom_push(key='df_data', value=df_transformed)
        except KeyError as e:
            print("KeyError during transformation:", e)  # Debugging log
            raise
    else:
        print("No data extracted from the API!")  # Debugging log
        raise ValueError("No data extracted from the API!")

# -------------------------------
# Step 3: Load - Insert Data into PostgreSQL
# -------------------------------

def load_data_to_postgres(**kwargs):
    # Get transformed data from XCom
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='transform_data', key='df_data')

    if df is not None:
        # Database connection parameters
        db_params = {
            "dbname": "dwh",
            "user": "ikeengr",
            "password": "DataEngineer247",
            "host": "89.40.0.150",
            "port": "5432"
        }

        # Create connection to PostgreSQL
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()

        # Create table if not exists
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

        # Insert DataFrame into PostgreSQL
        for index, row in df.iterrows():
            insert_query = """
            INSERT INTO random_mcq_game_quiz (question, correct_answer, option_1, option_2, option_3, option_4, reference, extra_type, extra_content, is_image)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            values = (row['question'], row['correct_answer'], row['option_1'], row['option_2'], row['option_3'], row['option_4'], row['reference'], row['extra_type'], row['extra_content'], row['is_image'])
            
            cur.execute(insert_query, values)

        # Commit the changes and close the connection
        conn.commit()
        cur.close()
        conn.close()

        print("Data loaded successfully into PostgreSQL!")
    else:
        raise ValueError("No data to load into PostgreSQL!")

# -------------------------------
# Define Airflow Tasks
# -------------------------------

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
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

# -------------------------------
# Define Task Dependencies
# -------------------------------

extract_task >> transform_task >> load_task
