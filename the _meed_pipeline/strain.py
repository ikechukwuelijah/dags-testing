from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import pandas as pd

# Default arguments for the DAG
default_args = {
    'owner': 'ikeengr',
    'start_date': days_ago(1),  # Start date set to one day ago
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'meed_strain_etl',
    default_args=default_args,
    description='ETL pipeline to extract weed strain data, transform it, and load it into PostgreSQL',
    schedule_interval='@monthly',  # Updated to run monthly
)

# Task 1: Extract data from the API
def extract_data(**kwargs):
    url = "https://weed-strain1.p.rapidapi.com/"
    querystring = {"ordering": "strain"}
    headers = {
        "x-rapidapi-key": "f38eae887bmsh5211e33c97c1c50p125cafjsnec52eb060a05",
        "x-rapidapi-host": "weed-strain1.p.rapidapi.com"
    }
    response = requests.get(url, headers=headers, params=querystring)
    data = response.json()
    # Push data to XCom for use in subsequent tasks
    kwargs['ti'].xcom_push(key='api_data', value=data)

# Task 2: Transform data into a Pandas DataFrame
def transform_data(**kwargs):
    # Pull data from XCom
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract_data', key='api_data')
    
    # Convert JSON data to DataFrame
    df = pd.DataFrame(data)
    
    # Handle data type issues (e.g., missing values, incorrect types)
    df.fillna(value={
        'thc': 'N/A',
        'cbd': 'N/A',
        'cbg': 'N/A',
        'indoorYieldInGramsMax': 0,
        'outdoorYieldInGramsMax': 0,
        'floweringWeeksMin': 0,
        'floweringWeeksMax': 0,
        'heightInInchesMin': 0,
        'heightInInchesMax': 0,
        'imgCreativeCommons': False
    }, inplace=True)
    
    # Push the transformed DataFrame to XCom
    ti.xcom_push(key='transformed_data', value=df.to_dict(orient='records'))

# Task 3: Load data into PostgreSQL
def load_data_to_postgres(**kwargs):
    # Pull transformed data from XCom
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='transform_data', key='transformed_data')
    df = pd.DataFrame(data)
    
    # Use PostgresHook to interact with PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id='postgres_dwh')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    
    # Create table if not exists
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
    
    # Insert data into the table
    for _, row in df.iterrows():
        cursor.execute('''
        INSERT INTO strains (
            id, strain, thc, cbd, cbg, strain_type, climate, difficulty, fungal_resistance,
            indoor_yield_max, outdoor_yield_max, flowering_weeks_min, flowering_weeks_max,
            height_inches_min, height_inches_max, good_effects, side_effects, img_thumb,
            img_attribution, img_attribution_link, img_creative_commons
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            strain = EXCLUDED.strain,
            thc = EXCLUDED.thc,
            cbd = EXCLUDED.cbd,
            cbg = EXCLUDED.cbg,
            strain_type = EXCLUDED.strain_type,
            climate = EXCLUDED.climate,
            difficulty = EXCLUDED.difficulty,
            fungal_resistance = EXCLUDED.fungal_resistance,
            indoor_yield_max = EXCLUDED.indoor_yield_max,
            outdoor_yield_max = EXCLUDED.outdoor_yield_max,
            flowering_weeks_min = EXCLUDED.flowering_weeks_min,
            flowering_weeks_max = EXCLUDED.flowering_weeks_max,
            height_inches_min = EXCLUDED.height_inches_min,
            height_inches_max = EXCLUDED.height_inches_max,
            good_effects = EXCLUDED.good_effects,
            side_effects = EXCLUDED.side_effects,
            img_thumb = EXCLUDED.img_thumb,
            img_attribution = EXCLUDED.img_attribution,
            img_attribution_link = EXCLUDED.img_attribution_link,
            img_creative_commons = EXCLUDED.img_creative_commons
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
    print("Data successfully loaded into PostgreSQL.")

# Define tasks
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    provide_context=True,
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

# Set task dependencies
extract_task >> transform_task >> load_task
