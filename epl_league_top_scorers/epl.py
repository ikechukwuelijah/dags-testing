import requests
import pandas as pd
import psycopg2
from psycopg2 import sql

# Your API key
API_KEY = 'db63bf0ee9433d6be33835f6066f606c'

# PostgreSQL connection details
DB_NAME = ''
DB_USER = ''
DB_PASSWORD = ''
DB_HOST = ''
DB_PORT = '5432'  # Default is 5432

# Function to fetch data from API-Football
def fetch_data(endpoint, params=None):
    url = f'https://v3.football.api-sports.io/{endpoint}'
    headers = {'x-apisports-key': API_KEY}
    response = requests.get(url, headers=headers, params=params)
    return response.json()

# Fetching Top Scorers Data
def get_top_scorers(league_id, season):
    endpoint = 'players/topscorers'
    params = {'league': league_id, 'season': season}
    data = fetch_data(endpoint, params)
    return data

# Transforming Data to a DataFrame
def transform_data(data):
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
    return df

# Function to upload DataFrame to PostgreSQL
def upload_to_postgres(df):
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
            host=DB_HOST, port=DB_PORT
        )
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

        # Insert DataFrame into PostgreSQL
        for _, row in df.iterrows():
            insert_query = """
            INSERT INTO epl_top_scorers (name, age, nationality, team, games, goals, assists, shots, shots_on_target, 
                                     passes, key_passes, dribbles, dribbles_success, yellow_cards, red_cards, penalties_scored)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            cursor.execute(insert_query, tuple(row))

        # Commit and close connection
        conn.commit()
        cursor.close()
        conn.close()
        print("Data successfully uploaded to PostgreSQL.")

    except Exception as e:
        print("Error uploading data to PostgreSQL:", e)

# Main execution
if __name__ == "__main__":
    league_id = 39  # Premier League ID
    season = 2023   # Season year

    data = get_top_scorers(league_id, season)
    df = transform_data(data)

    # Save data to CSV
    df.to_csv('top_scorers.csv', index=False)
    print("Data saved to top_scorers.csv")

    # Upload to PostgreSQL
    upload_to_postgres(df)
