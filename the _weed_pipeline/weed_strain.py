from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import pandas as pd
from datetime import datetime

def fetch_data(**kwargs):
    url = "https://weed-strain1.p.rapidapi.com/"
    querystring = {"ordering": "strain"}
    headers = {
        "x-rapidapi-key": "f38eae887bmsh5211e33c97c1c50p125cafjsnec52eb060a05",
        "x-rapidapi-host": "weed-strain1.p.rapidapi.com"
    }
    response = requests.get(url, headers=headers, params=querystring)
    data = response.json()
    df = pd.DataFrame(data)
    kwargs['ti'].xcom_push(key='strain_data', value=df.to_json())

def load_data_to_postgres(**kwargs):
    ti = kwargs['ti']
    data_json = ti.xcom_pull(task_ids='fetch_data', key='strain_data')
    df = pd.read_json(data_json)
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_dwh')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS strains (
        id SERIAL PRIMARY KEY,
        strain TEXT,
        thc TEXT,
        cbd TEXT,
        cbg TEXT,
        strain_type TEXT,
        climate TEXT,
        difficulty TEXT,
        fungal_resistance TEXT,
        indoor_yield_max FLOAT,
        outdoor_yield_max FLOAT,
        flowering_weeks_min FLOAT,
        flowering_weeks_max FLOAT,
        height_inches_min FLOAT,
        height_inches_max FLOAT,
        good_effects TEXT,
        side_effects TEXT,
        img_thumb TEXT,
        img_attribution TEXT,
        img_attribution_link TEXT,
        img_creative_commons BOOLEAN
    );
    '''
    cursor.execute(create_table_query)
    conn.commit()
    
    for _, row in df.iterrows():
        cursor.execute('''
            INSERT INTO strains (id, strain, thc, cbd, cbg, strain_type, climate, difficulty, fungal_resistance,
                                indoor_yield_max, outdoor_yield_max, flowering_weeks_min, flowering_weeks_max,
                                height_inches_min, height_inches_max, good_effects, side_effects, img_thumb,
                                img_attribution, img_attribution_link, img_creative_commons)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', (
            row['id'], row['strain'], row['thc'], row['cbd'], row['cbg'], row['strainType'], row['climate'],
            row['difficulty'], row['fungalResistance'], row['indoorYieldInGramsMax'], row['outdoorYieldInGramsMax'],
            row['floweringWeeksMin'], row['floweringWeeksMax'], row['heightInInchesMin'], row['heightInInchesMax'],
            row['goodEffects'], row['sideEffects'], row['imgThumb'], row['imgAttribution'],
            row['imgAttributionLink'], row['imgCreativeCommons']
        ))
    
    conn.commit()
    cursor.close()
    conn.close()

default_args = {
    'owner': 'ikeengr',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 20),
    'retries': 1,
}

dag = DAG(
    'strains_etl',
    default_args=default_args,
    schedule_interval='@monthly',
    catchup=False
)

fetch_task = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_data,
    provide_context=True,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_data_to_postgres',
    python_callable=load_data_to_postgres,
    provide_context=True,
    dag=dag
)

fetch_task >> load_task