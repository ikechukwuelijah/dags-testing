from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import requests
import pandas as pd
import json

# Define default arguments for the DAG
default_args = {
    'owner': 'Ik',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG with weekly schedule
dag = DAG(
    'aliexpress_sex_toys',
    default_args=default_args,
    description='Fetch data from AliExpress API and load into PostgreSQL using XCom',
    schedule_interval="@weekly",  # Runs every week
    catchup=False
)

# Task 1: Fetch data from API and store it in XCom
def fetch_data_from_api(**kwargs):
    url = "https://aliexpress-business-api.p.rapidapi.com/textsearch.php"
    querystring = {
        "keyWord": "sex toy",
        "pageSize": "20",
        "pageIndex": "1",
        "country": "US",
        "currency": "USD",
        "lang": "en",
        "filter": "orders",
        "sortBy": "asc"
    }
    headers = {
        "x-rapidapi-key": "b138837176msh66610dbb568050ap1aec3ejsn32d522c6ac16",
        "x-rapidapi-host": "aliexpress-business-api.p.rapidapi.com"
    }
    
    response = requests.get(url, headers=headers, params=querystring)

    if response.status_code == 200:
        data = response.json()
        # Push data to XCom for next task
        kwargs['ti'].xcom_push(key='aliexpress_data', value=data['data']['itemList'])
        return "Data fetched and stored in XCom"
    else:
        raise Exception(f"API request failed with status code {response.status_code}")

# Task 2: Transform data using XCom
def transform_data(**kwargs):
    ti = kwargs['ti']
    raw_data = ti.xcom_pull(task_ids='fetch_data_from_api', key='aliexpress_data')

    if not raw_data:
        raise ValueError("No data received from XCom")

    # Convert raw data into a DataFrame
    df = pd.DataFrame(raw_data)

    # Convert DataFrame to a list of dictionaries for XCom
    transformed_data = df.to_dict(orient='records')

    # Push transformed data to XCom
    ti.xcom_push(key='transformed_data', value=transformed_data)

    return "Data transformed and stored in XCom"

# Task 3: Load transformed data into PostgreSQL
def load_data_to_postgres(**kwargs):
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform_data', key='transformed_data')

    if not transformed_data:
        raise ValueError("No transformed data received from XCom")

    # Establish PostgreSQL connection using PostgresHook
    postgres_hook = PostgresHook(postgres_conn_id="postgres_dwh")  # Use Airflow connection ID
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # Create table if not exists
    create_table_query = """
    CREATE TABLE IF NOT EXISTS sex_toy (
        itemId VARCHAR PRIMARY KEY,
        title TEXT,
        originalPrice FLOAT,
        originalPriceCurrency VARCHAR(10),
        salePrice FLOAT,
        salePriceCurrency VARCHAR(10),
        discount VARCHAR(10),
        itemMainPic TEXT,
        score VARCHAR(10),
        targetSalePrice FLOAT,
        targetOriginalPrice FLOAT,
        cateId TEXT,
        orders TEXT
    );
    """
    cursor.execute(create_table_query)
    conn.commit()

    # Insert data into table
    insert_query = """
    INSERT INTO sex_toy (
        itemId, title, originalPrice, originalPriceCurrency, salePrice, salePriceCurrency, 
        discount, itemMainPic, score, targetSalePrice, targetOriginalPrice, cateId, orders
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (itemId) DO NOTHING;
    """

    # Convert transformed data into tuples for insertion
    records = [(item['itemId'], item['title'], item['originalPrice'], item['originalPriceCurrency'],
                item['salePrice'], item['salePriceCurrency'], item['discount'], item['itemMainPic'],
                item['score'], item['targetSalePrice'], item['targetOriginalPrice'],
                item['cateId'], item['orders']) for item in transformed_data]

    # Execute batch insert
    cursor.executemany(insert_query, records)
    conn.commit()

    cursor.close()
    conn.close()
    return "Data loaded into PostgreSQL successfully"

# Define Airflow tasks
fetch_task = PythonOperator(
    task_id='fetch_data_from_api',
    python_callable=fetch_data_from_api,
    provide_context=True,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_data_to_postgres',
    python_callable=load_data_to_postgres,
    provide_context=True,
    dag=dag
)

# Define task dependencies
fetch_task >> transform_task >> load_task
