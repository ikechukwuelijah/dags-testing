from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import pandas as pd
from datetime import datetime, timedelta

def fetch_all_countries():
    """Fetch data from REST Countries API."""
    url = 'https://restcountries.com/v3.1/all'
    response = requests.get(url)
    return response.json()

def transform_data(**kwargs):
    """Transform JSON data into a structured format using Pandas."""
    data = kwargs['ti'].xcom_pull(task_ids='fetch_data')
    
    countries = []
    for country in data:
        country_info = {
            'Name': country.get('name', {}).get('common', 'N/A'),
            'Capital': country.get('capital', ['N/A'])[0],
            'Region': country.get('region', 'N/A'),
            'Subregion': country.get('subregion', 'N/A'),
            'Population': country.get('population', 0),
            'Area': country.get('area', 0),
            'Languages': ', '.join(country.get('languages', {}).values()) if 'languages' in country else 'N/A',
            'Currencies': ', '.join([currency['name'] for currency in country.get('currencies', {}).values()]) if 'currencies' in country else 'N/A',
            'Flag': country.get('flags', {}).get('png', 'N/A')
        }
        countries.append(country_info)
    
    df = pd.DataFrame(countries)
    kwargs['ti'].xcom_push(key='transformed_data', value=df.to_dict(orient='records'))

def upload_data_to_postgres(**kwargs):
    """Upload transformed data to PostgreSQL using PostgresHook."""
    pg_hook = PostgresHook(postgres_conn_id='postgres_dwh')
    conn = pg_hook.get_conn()
    cur = conn.cursor()
    
    # Create table if not exists
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS countries (
        name TEXT UNIQUE,
        capital TEXT,
        region TEXT,
        subregion TEXT,
        population BIGINT,
        area REAL,
        languages TEXT,
        currencies TEXT,
        flag TEXT
    );
    '''
    cur.execute(create_table_query)
    
    # Insert transformed data
    data = kwargs['ti'].xcom_pull(task_ids='transform_data', key='transformed_data')
    for row in data:
        insert_query = '''
        INSERT INTO countries (name, capital, region, subregion, population, area, languages, currencies, flag)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (name) DO UPDATE 
        SET capital = EXCLUDED.capital,
            region = EXCLUDED.region,
            subregion = EXCLUDED.subregion,
            population = EXCLUDED.population,
            area = EXCLUDED.area,
            languages = EXCLUDED.languages,
            currencies = EXCLUDED.currencies,
            flag = EXCLUDED.flag;
        '''
        cur.execute(insert_query, (
            row['Name'], row['Capital'], row['Region'], row['Subregion'], row['Population'], 
            row['Area'], row['Languages'], row['Currencies'], row['Flag']
        ))
    
    conn.commit()
    cur.close()
    conn.close()

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
with DAG(
    'country_data_pipeline',
    default_args=default_args,
    description='A DAG to fetch, transform, and upload country data weekly',
    schedule_interval='@weekly',
    catchup=False
) as dag:
    
    fetch_data = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_all_countries,
        provide_context=True
    )
    
    transform_data_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True
    )
    
    upload_data_task = PythonOperator(
        task_id='upload_data',
        python_callable=upload_data_to_postgres,
        provide_context=True
    )
    
    fetch_data >> transform_data_task >> upload_data_task
