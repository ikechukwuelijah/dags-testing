from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
import pandas as pd
import numpy as np
import psycopg2
from datetime import datetime, timedelta

# Install required package first
# pip install pyarrow

default_args = {
    'owner': 'Ik',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 11),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'random_mcq_game_quiz',
    default_args=default_args,
    description='ETL for random MCQ game quiz',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

def extract_data():
    # ... [unchanged extract code] ...

def transform_data(**kwargs):
    ti = kwargs['ti']
    response_data = ti.xcom_pull(task_ids='extract_data')

    if response_data:
        df = pd.json_normalize(response_data)
        
        df_transformed = df[[
            'question', 'options.correct', 'options.incorrect',
            'reference', 'extra.type', 'extra.content', 'options.is_image'
        ]].copy()
        
        # Process options
        df_transformed['all_options'] = df_transformed.apply(
            lambda row: [row['options.correct']] + row['options.incorrect'],
            axis=1
        )
        
        # Handle option shuffling and splitting
        shuffled = df_transformed['all_options'].apply(
            lambda x: pd.Series(np.random.permutation(x))
        )
        df_transformed[['option_1','option_2','option_3','option_4']] = shuffled
        
        # Cleanup and rename
        df_transformed = df_transformed.drop(columns=['options.incorrect', 'all_options'])
        df_transformed = df_transformed.rename(columns={
            'options.correct': 'correct_answer',
            'extra.type': 'extra_type',
            'extra.content': 'extra_content',
            'options.is_image': 'is_image'
        })
        
        # Ensure correct column order
        df_transformed = df_transformed[[
            'question', 'correct_answer', 'option_1', 'option_2',
            'option_3', 'option_4', 'reference', 'extra_type',
            'extra_content', 'is_image'
        ]]
        
        # Convert to JSON-friendly format
        ti.xcom_push(key='df_data', value=df_transformed.to_dict('records'))
    else:
        raise ValueError("No data extracted!")

def load_data_to_postgres(**kwargs):
    ti = kwargs['ti']
    records = ti.xcom_pull(task_ids='transform_data', key='df_data')
    
    if records:
        df = pd.DataFrame(records)
        
        conn = psycopg2.connect(
            dbname="dwh", user="ikeengr",
            password="DataEngineer247", host="89.40.0.150", port="5432"
        )
        
        try:
            with conn.cursor() as cur:
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
                
                # Convert boolean column
                df['is_image'] = df['is_image'].astype(bool)
                
                # Use execute_batch for bulk insert
                psycopg2.extras.execute_batch(
                    cur,
                    """INSERT INTO random_mcq_game_quiz (
                        question, correct_answer, option_1, option_2,
                        option_3, option_4, reference, extra_type,
                        extra_content, is_image
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    df.to_records(index=False).tolist()
                )
            conn.commit()
            print(f"Inserted {len(df)} records successfully!")
        finally:
            conn.close()
    else:
        raise ValueError("No data to load!")

# Task definitions remain the same
extract_task = PythonOperator(...)
transform_task = PythonOperator(...)
load_task = PythonOperator(...)

extract_task >> transform_task >> load_task