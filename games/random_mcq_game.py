from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
import pandas as pd
import numpy as np
import psycopg2
from datetime import datetime, timedelta

# -------------------------------
# Define the DAG arguments
# -------------------------------
default_args = {
    'owner': 'Ik',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 11),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# -------------------------------
# Define the DAG
# -------------------------------
dag = DAG(
    'random_mcq_game_quiz',
    default_args=default_args,
    description='ETL for random MCQ game quiz',
    schedule_interval=timedelta(days=1),
    catchup=False,
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
    
    if response_data['status'] == 'ok':
        return response_data['data']
    else:
        return None

# -------------------------------
# Step 2: Transform - Process and Format Data (FIXED)
# -------------------------------
def transform_data(**kwargs):
    ti = kwargs['ti']
    response_data = ti.xcom_pull(task_ids='extract_data')

    if response_data:
        df = pd.json_normalize(response_data)
        
        # Create temporary DataFrame with required fields
        df_transformed = df[[
            'question', 
            'options.correct', 
            'options.incorrect', 
            'reference', 
            'extra.type', 
            'extra.content', 
            'options.is_image'
        ]].copy()
        
        # Combine correct + incorrect answers and shuffle
        df_transformed['all_options'] = df_transformed.apply(
            lambda row: [row['options.correct']] + row['options.incorrect'],
            axis=1
        )
        
        # Shuffle options and split into 4 columns (FIXED)
        shuffled_options = df_transformed['all_options'].apply(
            lambda x: pd.Series(np.random.permutation(x))
        )
        df_transformed[['option_1', 'option_2', 'option_3', 'option_4']] = shuffled_options
        
        # Cleanup temporary columns
        df_transformed.drop(
            columns=['options.incorrect', 'all_options'], 
            inplace=True
        )
        
        # Rename remaining columns
        df_transformed.rename(columns={
            'options.correct': 'correct_answer',
            'extra.type': 'extra_type',
            'extra.content': 'extra_content',
            'options.is_image': 'is_image'
        }, inplace=True)
        
        # Ensure final column order
        df_transformed = df_transformed[[
            'question', 
            'correct_answer', 
            'option_1', 
            'option_2', 
            'option_3', 
            'option_4',
            'reference', 
            'extra_type', 
            'extra_content', 
            'is_image'
        ]]
        
        ti.xcom_push(key='df_data', value=df_transformed)
    else:
        raise ValueError("No data extracted from the API!")

# -------------------------------
# Step 3: Load - Insert into PostgreSQL (OPTIMIZED)
# -------------------------------
def load_data_to_postgres(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='transform_data', key='df_data')

    if df is not None:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            dbname="dwh",
            user="ikeengr",
            password="DataEngineer247",
            host="89.40.0.150",
            port="5432"
        )
        
        try:
            with conn.cursor() as cur:
                # Create table if not exists
                cur.execute("""
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
                    )
                """)
                
                # Convert DataFrame to list of tuples
                data_tuples = [
                    (
                        row['question'], row['correct_answer'],
                        row['option_1'], row['option_2'],
                        row['option_3'], row['option_4'],
                        row['reference'], row['extra_type'],
                        row['extra_content'], row['is_image']
                    )
                    for _, row in df.iterrows()
                ]
                
                # Bulk insert using execute_batch
                psycopg2.extras.execute_batch(
                    cur,
                    """INSERT INTO random_mcq_game_quiz (
                        question, correct_answer,
                        option_1, option_2, option_3, option_4,
                        reference, extra_type, extra_content, is_image
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    data_tuples
                )
            
            conn.commit()
            print(f"Inserted {len(data_tuples)} records successfully!")
            
        except Exception as e:
            conn.rollback()
            raise RuntimeError(f"Database operation failed: {str(e)}")
        
        finally:
            if conn is not None:
                conn.close()
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

extract_task >> transform_task >> load_task

