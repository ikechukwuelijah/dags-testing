from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import pandas as pd


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
    schedule_interval='@weekly',  # Runs weekly
    catchup=False
)

# Function to fetch data from API-Football
def fetch_data():
    url = 'https://v3.football.api-sports.io/players/topscorers'
    params = {'league': 39, 'season': 2023}  # Premier League, Season 2023
    headers = {'x-apisports-key': API_KEY}
    response = requests.get(url, headers=headers, params=params)
    data = response.json()
    return data

# Function to transform data into a Pandas DataFrame
def transform_data():
    data = fetch_data()
    players = data.get('response', [])
    extracted_data = []

    for player in players:
        player_info = player.get('player', {})
        statistics = player.get('statistics', [])[0]  # First entry has main stats

        extracted_data.append({
            'name': player_info.get('name'),
            'age': player_info.get('age'),
            'nationality': player_info.get('nationality'),
            'team': statistics.get('team', {}).get('name'),
            'games': statistics.get('games', {}).get('appearences'),
            'goals': statistics.get('goals', {}).get('total'),
            'assists': statistics.get('goals', {}).get('assists'),
            'shots': statistics.get('shots', {}).get('total'),
            'shots_on_target': statistics.get('shots', {}).get('on'),
            'passes': statistics.get('passes', {}).get('total'),
            'key_passes': statistics.get('passes', {}).get('key'),
            'dribbles': statistics.get('dribbles', {}).get('attempts'),
            'dribbles_success': statistics.get('dribbles', {}).get('success'),
            'yellow_cards': statistics.get('cards', {}).get('yellow'),
            'red_cards': statistics.get('cards', {}).get('red'),
            'penalties_scored': statistics.get('penalty', {}).get('scored'),
        })

    df = pd.DataFrame(extracted_data)
    df.to_csv('/tmp/top_scorers.csv', index=False)  # Save to temp file

# Function to upload DataFrame to PostgreSQL using PostgresHook
def upload_to_postgres():
    try:
        hook = PostgresHook(postgres_conn_id='postgres_dwh')
        conn = hook.get_conn()
        cursor = conn.cursor()

        # Create table if not exists
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

        # Read CSV and insert into PostgreSQL
        df = pd.read_csv('/tmp/top_scorers.csv')
        for _, row in df.iterrows():
            insert_query = """
            INSERT INTO epl_top_scorers (name, age, nationality, team, games, goals, assists, shots, shots_on_target, 
                                         passes, key_passes, dribbles, dribbles_success, yellow_cards, red_cards, penalties_scored)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            cursor.execute(insert_query, tuple(row))

        conn.commit()
        cursor.close()
        conn.close()
        print("Data successfully uploaded to PostgreSQL.")

    except Exception as e:
        print("Error uploading data to PostgreSQL:", e)

# Define Airflow tasks
fetch_task = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

upload_task = PythonOperator(
    task_id='upload_to_postgres',
    python_callable=upload_to_postgres,
    dag=dag,
)

# Define task dependencies
fetch_task >> transform_task >> upload_task