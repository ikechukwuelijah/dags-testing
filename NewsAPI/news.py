from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import pandas as pd

# Default arguments for the DAG
default_args = {
    "owner": "Ik",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 25),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "news_api_to_postgres",
    default_args=default_args,
    description="Fetch tech news and store in PostgreSQL",
    schedule_interval=timedelta(days=1),
    catchup=False,
)

# Extract: Fetch news data from API
def extract_news(ti):
    URL = f"https://newsapi.org/v2/top-headlines?country=us&category=technology&apiKey=9d64ba92867247f2a6c57a04a7eebc78"
    
    response = requests.get(URL)
    if response.status_code == 200:
        news_data = response.json()
        ti.xcom_push(key="news_data", value=news_data)
    else:
        raise ValueError(f"Error fetching data: {response.status_code}")

extract_task = PythonOperator(
    task_id="extract_news",
    python_callable=extract_news,
    provide_context=True,
    dag=dag,
)

# Transform: Process the extracted data
def transform_news(ti):
    news_data = ti.xcom_pull(task_ids="extract_news", key="news_data")
    articles = news_data.get("articles", [])
    
    if articles:
        df = pd.DataFrame(articles, columns=["title", "source", "url"])
        df["source"] = df["source"].apply(lambda x: x["name"] if isinstance(x, dict) else x)
        ti.xcom_push(key="news_df", value=df.to_dict(orient="records"))
    else:
        raise ValueError("No news articles found.")

transform_task = PythonOperator(
    task_id="transform_news",
    python_callable=transform_news,
    provide_context=True,
    dag=dag,
)

# Load: Insert data into PostgreSQL
def load_news(ti):
    news_records = ti.xcom_pull(task_ids="transform_news", key="news_df")
    if not news_records:
        raise ValueError("No data to insert.")
    
    pg_hook = PostgresHook(postgres_conn_id="postgres_dwh")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    for record in news_records:
        try:
            cursor.execute(
                """
                INSERT INTO tech_news (title, source, url)
                VALUES (%s, %s, %s)
                ON CONFLICT (url) DO NOTHING;
                """,
                (record["title"], record["source"], record["url"])
            )
        except Exception as e:
            print(f"Error inserting row: {e}")
            conn.rollback()
        else:
            conn.commit()
    
    cursor.close()
    conn.close()

load_task = PythonOperator(
    task_id="load_news",
    python_callable=load_news,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
extract_task >> transform_task >> load_task
