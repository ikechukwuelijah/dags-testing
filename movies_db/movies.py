from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import requests
import pandas as pd
import json
from datetime import datetime, timedelta

# Default DAG arguments
default_args = {
    "owner": "Ik",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 10),  # Adjust the start date if needed
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    "movies_data_pipeline",
    default_args=default_args,
    description="Fetch movie data from API and load into PostgreSQL using XCom",
    schedule_interval="@daily",  # Runs every day at midnight
    catchup=False,
)

# Function to fetch movie data and push to XCom
def fetch_movies(**kwargs):
    url = "https://movies-tv-shows-database.p.rapidapi.com/"
    querystring = {"year": "2025", "page": "1"}
    headers = {
        "x-rapidapi-key": "b138837176msh66610dbb568050ap1aec3ejsn32d522c6ac16",
        "x-rapidapi-host": "movies-tv-shows-database.p.rapidapi.com",
        "Type": "get-shows-byyear"
    }

    response = requests.get(url, headers=headers, params=querystring)

    if response.status_code == 200:
        data = response.json()
        if "movie_results" in data:
            movies = [{"title": m["title"], "year": m["year"], "imdb_id": m["imdb_id"]} for m in data["movie_results"]]
            kwargs["ti"].xcom_push(key="movies_data", value=movies)  # Push data to XCom
            print("Movie data pushed to XCom")
        else:
            print("No movie data found!")
    else:
        print(f"Failed to fetch data: {response.status_code} - {response.text}")

# Function to load data into PostgreSQL using XCom
def load_movies_to_postgres(**kwargs):
    postgres_hook = PostgresHook(postgres_conn_id="postgres_dwh")  # Set in Airflow UI
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # Create table if not exists
    create_table_query = """
    CREATE TABLE IF NOT EXISTS movies (
        id SERIAL PRIMARY KEY,
        title TEXT,
        year INT,
        imdb_id TEXT UNIQUE
    );
    """
    cursor.execute(create_table_query)
    conn.commit()

    # Retrieve data from XCom
    ti = kwargs["ti"]
    movies_data = ti.xcom_pull(task_ids="fetch_movies", key="movies_data")

    if not movies_data:
        print("No movie data found in XCom!")
        return

    # Insert data into PostgreSQL
    insert_query = """
    INSERT INTO movies (title, year, imdb_id) 
    VALUES (%s, %s, %s) 
    ON CONFLICT (imdb_id) DO NOTHING;
    """
    movie_tuples = [(m["title"], int(m["year"]), m["imdb_id"]) for m in movies_data]

    cursor.executemany(insert_query, movie_tuples)
    conn.commit()

    cursor.close()
    conn.close()
    print("Data successfully loaded into PostgreSQL!")

# Define tasks
fetch_task = PythonOperator(
    task_id="fetch_movies",
    python_callable=fetch_movies,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id="load_movies_to_postgres",
    python_callable=load_movies_to_postgres,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
fetch_task >> load_task
