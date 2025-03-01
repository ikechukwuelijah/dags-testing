from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import xml.etree.ElementTree as ET
import pandas as pd

# API URL for fetching EV charging station data
API_URL = "https://api.openchargemap.io/v3/poi/?output=kml&countrycode=CA&maxresults=5000&key=c8548da8-d611-4aea-b4e2-bfe17944b989"

# Define default arguments for the DAG
default_args = {
    'owner': 'Ik',
    'start_date': datetime(2025, 3, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the Airflow DAG
dag = DAG(
    'uk_ev_charging_stations',
    default_args=default_args,
    schedule_interval='@weekly',  # Runs weekly
    catchup=False  # Prevents running missed historical DAGs
)

# Function to fetch KML data from Open Charge Map API
def fetch_data(**kwargs):
    try:
        response = requests.get(API_URL, timeout=30)
        response.raise_for_status()
        kwargs['ti'].xcom_push(key='raw_kml', value=response.content.decode())  # Store KML data in XCom
    except requests.RequestException as e:
        raise ValueError(f"Error fetching data: {e}")

# Function to parse KML data into a Pandas DataFrame
def parse_kml(**kwargs):
    ti = kwargs['ti']
    kml_data = ti.xcom_pull(key='raw_kml', task_ids='fetch_data')

    if not kml_data:
        raise ValueError("No data found in KML response.")

    try:
        root = ET.fromstring(kml_data)
        namespace = "{http://www.opengis.net/kml/2.2}"
        stations = []

        for placemark in root.findall(f'.//{namespace}Placemark'):
            name_elem = placemark.find(f'.//{namespace}name')
            coord_elem = placemark.find(f'.//{namespace}coordinates')
            description_elem = placemark.find(f'.//{namespace}description')

            if name_elem is not None and coord_elem is not None:
                name = name_elem.text
                coordinates = coord_elem.text.strip()
                description = description_elem.text if description_elem is not None else ""

                # Extract longitude and latitude
                lon, lat, *_ = coordinates.split(",")
                stations.append({"name": name, "latitude": float(lat), "longitude": float(lon), "description": description})

        df = pd.DataFrame(stations)
        ti.xcom_push(key='transformed_data', value=df.to_json())  # Store DataFrame in XCom
    except ET.ParseError as e:
        raise ValueError(f"Error parsing KML: {e}")

# Function to upload data to PostgreSQL using PostgresHook
def upload_to_postgres(**kwargs):
    ti = kwargs['ti']
    df_json = ti.xcom_pull(key='transformed_data', task_ids='parse_kml')

    if not df_json:
        raise ValueError("No transformed data found.")

    df = pd.read_json(df_json)  # Convert JSON back to DataFrame

    try:
        # Connect to PostgreSQL using Airflow's PostgresHook
        hook = PostgresHook(postgres_conn_id='postgres_dwh')
        conn = hook.get_conn()
        cursor = conn.cursor()

        # Create table if not exists
        create_table_query = """
        CREATE TABLE IF NOT EXISTS ev_charging_stations (
            id SERIAL PRIMARY KEY,
            name TEXT,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            description TEXT
        );
        """
        cursor.execute(create_table_query)
        conn.commit()

        # Insert data into PostgreSQL
        for _, row in df.iterrows():
            insert_query = """
            INSERT INTO ev_charging_stations (name, latitude, longitude, description)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
            """
            cursor.execute(insert_query, (row["name"], row["latitude"], row["longitude"], row["description"]))

        conn.commit()
        cursor.close()
        conn.close()
        print("Data uploaded successfully to PostgreSQL.")

    except Exception as e:
        raise ValueError(f"Error uploading data to PostgreSQL: {e}")

# Define Airflow tasks
fetch_task = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_data,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='parse_kml',
    python_callable=parse_kml,
    provide_context=True,
    dag=dag,
)

upload_task = PythonOperator(
    task_id='upload_to_postgres',
    python_callable=upload_to_postgres,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
fetch_task >> transform_task >> upload_task
