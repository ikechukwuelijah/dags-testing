from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import requests
import pandas as pd

# API Configuration
API_URL = "https://the-weed-db.p.rapidapi.com/api/strains"
API_HEADERS = {
    "x-rapidapi-key": "f38eae887bmsh5211e33c97c1c50p125cafjsnec52eb060a05",
    "x-rapidapi-host": "the-weed-db.p.rapidapi.com"
}
QUERY_PARAMS = {"growDifficulty": "difficult"}

# DAG Definition
with DAG(
    "difficult_grow_strains_etl",
    schedule_interval="0 0 1 * *",  # Runs monthly on the 1st at midnight
    start_date=days_ago(1),
    catchup=False,
    tags=["ETL", "PostgreSQL"],
    default_args={
        "owner": "ikeengr",  # ✅ DAG Owner Set
        "depends_on_past": False,
        "retries": 1,
    }
):

    @task()
    def extract_data():
        """Extracts 'difficult grow' strains from API and returns JSON via XCom."""
        response = requests.get(API_URL, headers=API_HEADERS, params=QUERY_PARAMS)
        if response.status_code == 200:
            return response.json()
        else:
            raise ValueError(f"API Request Failed with status code: {response.status_code}")

    @task()
    def transform_data(data):
        """Transforms API JSON response into a list of tuples for PostgreSQL."""
        df = pd.DataFrame(data)

        transformed_data = [
            (
                row.get('_id', None), 
                row.get('name', None),
                row.get('link', None),
                row.get('imageUrl', None),
                row.get('description', None),
                row.get('genetics', None),
                row.get('THC', None),
                row.get('CBD', None),
                row.get('parents', None),
                row.get('smellAndFlavour', None),
                row.get('effect', None),
                row.get('growEnvironments', None),
                row.get('growDifficulty', None),
                row.get('floweringType', None),
                row.get('floweringTime', None),
                row.get('harvestTimeOutdoor', None),
                row.get('yieldIndoor', None),
                row.get('yieldOutdoor', None),
                row.get('heightIndoor', None),
                row.get('heightOutdoor', None),
                row.get('fromSeedToHarvest', None)
            ) for _, row in df.iterrows()
        ]
        return transformed_data  # Stored in XCom

    @task()
    def load_data(records):
        """Loads transformed data into PostgreSQL using PostgresHook."""
        pg_hook = PostgresHook(postgres_conn_id="postgres_dwh")  # Airflow Connection ID
        conn = pg_hook.get_conn()
        cur = conn.cursor()

        # Create Table (if not exists)
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS difficult_grow_strains (
            id TEXT PRIMARY KEY,
            name TEXT,
            link TEXT,
            imageUrl TEXT,
            description TEXT,
            genetics TEXT,
            THC TEXT,
            CBD TEXT,
            parents TEXT,
            smellAndFlavour TEXT,
            effect TEXT,
            growEnvironments TEXT,
            growDifficulty TEXT,
            floweringType TEXT,
            floweringTime TEXT,
            harvestTimeOutdoor TEXT,
            yieldIndoor TEXT,
            yieldOutdoor TEXT,
            heightIndoor TEXT,
            heightOutdoor TEXT,
            fromSeedToHarvest TEXT
        );
        '''
        cur.execute(create_table_query)
        conn.commit()

        # Insert Data into Table
        insert_query = '''
        INSERT INTO difficult_grow_strains (id, name, link, imageUrl, description, genetics, THC, CBD, 
                                         parents, smellAndFlavour, effect, growEnvironments, growDifficulty, 
                                         floweringType, floweringTime, harvestTimeOutdoor, yieldIndoor, 
                                         yieldOutdoor, heightIndoor, heightOutdoor, fromSeedToHarvest)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING;
        '''
        
        # Batch Insert
        cur.executemany(insert_query, records)
        conn.commit()

        # Close connection
        cur.close()
        conn.close()

    # Task Dependencies
    raw_data = extract_data()
    transformed_data = transform_data(raw_data)
    load_data(transformed_data)
