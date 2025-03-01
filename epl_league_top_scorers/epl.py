from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import pandas as pd
import traceback

# Define default arguments for the DAG
default_args = {
    'owner': 'Ik',
    'start_date': datetime(2025, 3, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define the Airflow DAG
dag = DAG(
    'epl_top_scorers',
    default_args=default_args,
    schedule_interval='@weekly',
    catchup=False
)

# Function to fetch data from API-Football
def fetch_data():
    url = 'https://v3.football.api-sports.io/players/topscorers'
    params = {'league': 39, 'season': 2023}  # Premier League, Season 2023
    headers = {'x-apisports-key': 'db63bf0ee9433d6be33835f6066f606c'}
    
    response = requests.get(url, headers=headers, params=params)
    data = response.json()
    
    if response.status_code != 200:
        raise Exception(f"API request failed: {data}")
    
    print("✅ Successfully fetched data from API")
    return data

# Function to transform data into a Pandas DataFrame and return JSON
def transform_data(**kwargs):
    try:
        data = fetch_data()
        players = data.get('response', [])
        extracted_data = []

        for player in players:
            player_info = player.get('player', {})
            statistics = player.get('statistics', [{}])[0]  # Avoid index error

            extracted_data.append({
                'name': player_info.get('name'),
                'age': player_info.get('age', 0),
                'nationality': player_info.get('nationality', ''),
                'team': statistics.get('team', {}).get('name', ''),
                'games': statistics.get('games', {}).get('appearences', 0),
                'goals': statistics.get('goals', {}).get('total', 0),
                'assists': statistics.get('goals', {}).get('assists', 0),
                'shots': statistics.get('shots', {}).get('total', 0),
                'shots_on_target': statistics.get('shots', {}).get('on', 0),
                'passes': statistics.get('passes', {}).get('total', 0),
                'key_passes': statistics.get('passes', {}).get('key', 0),
                'dribbles': statistics.get('dribbles', {}).get('attempts', 0),
                'dribbles_success': statistics.get('dribbles', {}).get('success', 0),
                'yellow_cards': statistics.get('cards', {}).get('yellow', 0),
                'red_cards': statistics.get('cards', {}).get('red', 0),
                'penalties_scored': statistics.get('penalty', {}).get('scored', 0),
            })

        df = pd.DataFrame(extracted_data)

        # Convert DataFrame to JSON and push to XCom
        json_data = df.to_json(orient='records')
        kwargs['ti'].xcom_push(key='top_scorers_data', value=json_data)

        print("✅ Data transformation complete, pushed to XCom.")

    except Exception as e:
        print("❌ Error in transform_data")
        print(traceback.format_exc())

# Function to upload JSON data from XCom to PostgreSQL
def upload_to_postgres(**kwargs):
    try:
        hook = PostgresHook(postgres_conn_id='postgres_dwh')
        conn = hook.get_conn()
        cursor = conn.cursor()

        # Ensure table exists
        create_table_query = """
        CREATE TABLE IF NOT EXISTS epl_top_scorers (
            id SERIAL PRIMARY KEY,
            name TEXT,
            age INT,
            nationality TEXT,
            team TEXT,
            games INT,
            goals INT,
            assists INT,
            shots INT,
            shots_on_target INT,
            passes INT,
            key_passes INT,
            dribbles INT,
            dribbles_success INT,
            yellow_cards INT,
            red_cards INT,
            penalties_scored INT
        );
        """
        cursor.execute(create_table_query)
        conn.commit()

        # Retrieve JSON data from XCom
        ti = kwargs['ti']
        json_data = ti.xcom_pull(task_ids='transform_data', key='top_scorers_data')

        if not json_data:
            raise ValueError("❌ No data found in XCom for 'top_scorers_data'.")

        df = pd.read_json(json_data)

        # Replace NaN with 0
        df = df.fillna(0)

        # Insert data into PostgreSQL
        insert_query = """
        INSERT INTO epl_top_scorers (name, age, nationality, team, games, goals, assists, shots, shots_on_target, 
                                     passes, key_passes, dribbles, dribbles_success, yellow_cards, red_cards, penalties_scored)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        for _, row in df.iterrows():
            cursor.execute(insert_query, tuple(row))

        conn.commit()
        cursor.close()
        conn.close()
        print("✅ Data successfully uploaded to PostgreSQL.")

    except Exception as e:
        print("❌ Error in upload_to_postgres")
        print(traceback.format_exc())

# Define Airflow tasks
fetch_task = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,  # Enables XCom pushing
    dag=dag,
)

upload_task = PythonOperator(
    task_id='upload_to_postgres',
    python_callable=upload_to_postgres,
    provide_context=True,  # Enables XCom pulling
    dag=dag,
)

# Define task dependencies
fetch_task >> transform_task >> upload_task
