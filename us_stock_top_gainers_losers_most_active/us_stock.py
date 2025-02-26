from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import pandas as pd

# Alpha Vantage API Key
TABLE_NAME = "us_stock_market"

# Default arguments for DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 26),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    "us_stock_market",
    default_args=default_args,
    description="Fetch US stock market data and store in PostgreSQL",
    schedule_interval=timedelta(days=1),
    catchup=False,
)

# Extract data from Alpha Vantage API
def extract_data(**kwargs):
    url = f"https://www.alphavantage.co/query?function=TOP_GAINERS_LOSERS&apikey=D71T6RVK8CNYB0LS"
    response = requests.get(url)
    data = response.json()
    
    if "top_gainers" in data and "top_losers" in data and "most_actively_traded" in data:
        # Push extracted data to XCom
        kwargs['ti'].xcom_push(key="stock_data", value=data)
    else:
        raise ValueError("Invalid API response: Missing expected fields")

extract_task = PythonOperator(
    task_id="extract_stock_data",
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)

# Transform data into a structured format
def transform_data(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids="extract_stock_data", key="stock_data")

    # Convert API response to DataFrames
    top_gainers_df = pd.DataFrame(data.get("top_gainers", []))
    top_losers_df = pd.DataFrame(data.get("top_losers", []))
    most_actively_traded_df = pd.DataFrame(data.get("most_actively_traded", []))

    # Add category column
    top_gainers_df["category"] = "top_gainer"
    top_losers_df["category"] = "top_loser"
    most_actively_traded_df["category"] = "most_active"

    # Combine into a single DataFrame
    final_df = pd.concat([top_gainers_df, top_losers_df, most_actively_traded_df])

    # Standardize column names
    final_df.rename(columns={"ticker": "symbol", "price": "current_price", "change_percentage": "change_pct"}, inplace=True)

    # Push transformed data to XCom
    ti.xcom_push(key="transformed_stock_data", value=final_df.to_dict(orient="records"))

transform_task = PythonOperator(
    task_id="transform_stock_data",
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

# Load data into PostgreSQL using PostgresHook
def load_data(**kwargs):
    ti = kwargs['ti']
    stock_data = ti.xcom_pull(task_ids="transform_stock_data", key="transformed_stock_data")

    if not stock_data:
        raise ValueError("No data to insert into database")

    # Initialize Postgres Hook
    pg_hook = PostgresHook(postgres_conn_id="postgres_dwh")

    # SQL Query to create table if not exists
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        symbol TEXT PRIMARY KEY,
        current_price FLOAT,
        change_pct TEXT,
        volume BIGINT,
        category TEXT
    );
    """
    pg_hook.run(create_table_query)

    # Insert data into PostgreSQL
    insert_query = f"""
    INSERT INTO {TABLE_NAME} (symbol, current_price, change_pct, volume, category)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (symbol) DO UPDATE 
    SET current_price = EXCLUDED.current_price,
        change_pct = EXCLUDED.change_pct,
        volume = EXCLUDED.volume,
        category = EXCLUDED.category;
    """
    
    # Execute insert query for each record
    for record in stock_data:
        pg_hook.run(insert_query, parameters=(record["symbol"], record["current_price"], record["change_pct"], record["volume"], record["category"]))

    print(f"Successfully uploaded {len(stock_data)} records to {TABLE_NAME}")

load_task = PythonOperator(
    task_id="load_stock_data",
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

# Define DAG workflow
extract_task >> transform_task >> load_task
