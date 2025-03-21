from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import logging
from datetime import datetime
from psycopg2.extras import execute_values

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# DAG Configuration
default_args = {
    'owner': 'ikeengr',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 20),
    'retries': 1,
}

dag = DAG(
    'weed_strain_etl',
    default_args=default_args,
    schedule_interval='@monthly',
    catchup=False
)

# Step 1: Fetch Data (Assume fetch_data() is implemented correctly)
fetch_task = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_data,
    provide_context=True,
    dag=dag
)

# Step 2: Load Data to PostgreSQL
def load_data_to_postgres(**kwargs):
    ti = kwargs['ti']
    data_json = ti.xcom_pull(task_ids='fetch_data', key='strain_data')
    df = pd.read_json(data_json)

    # Connect to PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id='postgres_dwh')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Create Table if not Exists
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

    # Map API Fields to Table Columns
    column_mapping = {
        "strain": "strain",
        "thc": "thc",
        "cbd": "cbd",
        "cbg": "cbg",
        "strainType": "strain_type",
        "climate": "climate",
        "difficulty": "difficulty",
        "fungalResistance": "fungal_resistance",
        "indoorYieldInGramsMax": "indoor_yield_max",
        "outdoorYieldInGramsMax": "outdoor_yield_max",
        "floweringWeeksMin": "flowering_weeks_min",
        "floweringWeeksMax": "flowering_weeks_max",
        "heightInInchesMin": "height_inches_min",
        "heightInInchesMax": "height_inches_max",
        "goodEffects": "good_effects",
        "sideEffects": "side_effects",
        "imgThumb": "img_thumb",
        "imgAttribution": "img_attribution",
        "imgAttributionLink": "img_attribution_link",
        "imgCreativeCommons": "img_creative_commons"
    }
    
    df.rename(columns=column_mapping, inplace=True)

    # Convert empty strings to None for numeric fields
    numeric_columns = [
        "indoor_yield_max", "outdoor_yield_max", "flowering_weeks_min", 
        "flowering_weeks_max", "height_inches_min", "height_inches_max"
    ]

    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')  # Convert to float, set errors to NaN
        df[col] = df[col].replace({pd.NA: None})  # Replace NaN with None

    df.fillna("", inplace=True)  # Replace NaN with empty strings for non-numeric columns

    # Insert Data with Batch Processing
    insert_query = '''
    INSERT INTO strains (
        strain, thc, cbd, cbg, strain_type, climate, difficulty, fungal_resistance,
        indoor_yield_max, outdoor_yield_max, flowering_weeks_min, flowering_weeks_max,
        height_inches_min, height_inches_max, good_effects, side_effects, img_thumb,
        img_attribution, img_attribution_link, img_creative_commons
    ) VALUES %s
    ON CONFLICT DO NOTHING;
    '''

    records = [
        (
            row.strain, row.thc, row.cbd, row.cbg, row.strain_type, row.climate, row.difficulty,
            row.fungal_resistance, row.indoor_yield_max, row.outdoor_yield_max, row.flowering_weeks_min,
            row.flowering_weeks_max, row.height_inches_min, row.height_inches_max, row.good_effects,
            row.side_effects, row.img_thumb, row.img_attribution, row.img_attribution_link, row.img_creative_commons
        )
        for _, row in df.iterrows()
    ]

    execute_values(cursor, insert_query, records)
    conn.commit()

    cursor.close()
    conn.close()
    logging.info("✅ Data successfully loaded into PostgreSQL.")

load_task = PythonOperator(
    task_id='load_data_to_postgres',
    python_callable=load_data_to_postgres,
    provide_context=True,
    dag=dag
)

# Task Dependencies
fetch_task >> load_task
