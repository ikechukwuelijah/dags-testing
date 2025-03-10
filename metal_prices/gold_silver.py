from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
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
    'fetch_metal_prices',
    default_args=default_args,
    description='Fetch metal prices from API and store them in PostgreSQL',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Function to fetch metal prices from API
    def fetch_metal_prices():
        url = "https://gold-price-live.p.rapidapi.com/get_metal_prices"
        headers = {
            "x-rapidapi-key": "7b66ced988msh253ab4a526f3148p1eed78jsn4d8bcaa48242",  # Store securely in Airflow connections
            "x-rapidapi-host": "gold-price-live.p.rapidapi.com"
        }
        
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            print("API Response:", data)
            return data
        else:
            raise Exception(f"Failed to fetch data: {response.status_code} {response.text}")
    
    # Function to transform data into DataFrame
    def transform_metal_prices(**kwargs):
        ti = kwargs['ti']
        data = ti.xcom_pull(task_ids='fetch_metal_prices')
        
        df = pd.DataFrame(list(data.items()), columns=['Metal', 'Price'])
        print("Transformed DataFrame:\n", df)
        return df.to_dict('records')
    
    # Function to load data into PostgreSQL
    def load_metal_prices(**kwargs):
        hook = PostgresHook(postgres_conn_id='postgres_dwh')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        # Create table if it doesn't exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS metal_prices (
            id SERIAL PRIMARY KEY,
            metal VARCHAR(50),
            price NUMERIC
        );
        """
        cursor.execute(create_table_query)
        conn.commit()
        
        # Get transformed data
        ti = kwargs['ti']
        prices = ti.xcom_pull(task_ids='transform_metal_prices')
        
        if not prices:
            print("No data available to insert.")
            return
        
        # Insert data into table
        insert_query = """
        INSERT INTO metal_prices (metal, price)
        VALUES (%s, %s)
        """
        
        for price in prices:
            cursor.execute(insert_query, (price['Metal'], price['Price']))
        
        conn.commit()
        cursor.close()
        conn.close()
        print("Data successfully loaded into PostgreSQL database.")
    
    # Define tasks
    fetch_task = PythonOperator(
        task_id='fetch_metal_prices',
        python_callable=fetch_metal_prices
    )
    
    transform_task = PythonOperator(
        task_id='transform_metal_prices',
        python_callable=transform_metal_prices,
        provide_context=True
    )
    
    load_task = PythonOperator(
        task_id='load_metal_prices',
        python_callable=load_metal_prices,
        provide_context=True
    )
    
    # Set task dependencies
    fetch_task >> transform_task >> load_task
