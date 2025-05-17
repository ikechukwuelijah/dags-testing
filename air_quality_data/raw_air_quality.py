#%%
import requests
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
from psycopg2 import sql


# === Configuration: OpenWeatherMap API ===
API_KEY = "fc284336c861c52e8185c63082114ad5"  # Replace with your actual API key
LAT = 51.5074  # Latitude of London
LON = -0.1278  # Longitude of London

# === PostgreSQL DB Config ===
DB_CONFIG = {
    "dbname": "dwh",
    "user": "ikeengr",
    "password": "DataEngineer247",
    "host": "89.40.0.150",
    "port": "5432"
}

TABLE_NAME = "raw_air_quality"

# === Calculate Yesterday's Start and End in UNIX Timestamp ===
now = datetime.utcnow()
yesterday = now - timedelta(days=1)

start_of_yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
end_of_yesterday = yesterday.replace(hour=23, minute=59, second=59, microsecond=0)

start_timestamp = int(start_of_yesterday.timestamp())
end_timestamp = int(end_of_yesterday.timestamp())

# === API URL and Parameters ===
url = "http://api.openweathermap.org/data/2.5/air_pollution/history"
params = {
    "lat": LAT,
    "lon": LON,
    "start": start_timestamp,
    "end": end_timestamp,
    "appid": API_KEY
}

# === Make Request ===
try:
    print(f"Fetching data from {start_of_yesterday} to {end_of_yesterday} UTC...")
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()

    # === Transform JSON to DataFrame ===
    rows = []
    for entry in data["list"]:
        row = {
            "timestamp": datetime.utcfromtimestamp(entry["dt"]),
            "aqi": entry["main"]["aqi"]
        }
        row.update(entry["components"])
        rows.append(row)

    df = pd.DataFrame(rows)

    # Optional: Rename columns for clarity
    df.rename(columns={
        "pm2_5": "pm25",
        "pm10": "pm10"
    }, inplace=True)

    # Display the DataFrame
    print("\n📊 Transformed DataFrame:")
    print(df.head())

    # === Load to PostgreSQL ===
    print("\n🔌 Connecting to PostgreSQL...")

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Create table if not exists
    create_table_query = sql.SQL("""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        timestamp TIMESTAMP NOT NULL,
        aqi INTEGER,
        co FLOAT,
        no FLOAT,
        no2 FLOAT,
        o3 FLOAT,
        so2 FLOAT,
        pm25 FLOAT,
        pm10 FLOAT,
        nh3 FLOAT
    )
    """).format(table_name=sql.Identifier(TABLE_NAME))

    cur.execute(create_table_query)
    conn.commit()

    # Insert DataFrame rows
    insert_query = sql.SQL("""
    INSERT INTO {table_name} (timestamp, aqi, co, no, no2, o3, so2, pm25, pm10, nh3)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """).format(table_name=sql.Identifier(TABLE_NAME))

    for _, row in df.iterrows():
        cur.execute(insert_query, (
            row['timestamp'],
            row['aqi'],
            row['co'],
            row['no'],
            row['no2'],
            row['o3'],
            row['so2'],
            row.get('pm25'),
            row.get('pm10'),
            row['nh3']
        ))

    conn.commit()
    cur.close()
    conn.close()

    print("✅ Data successfully loaded to PostgreSQL!")

except requests.exceptions.HTTPError as err:
    print(f"❌ HTTP error occurred: {err}")
    print("Response text:", response.text)
except requests.exceptions.RequestException as err:
    print(f"❌ Request failed: {err}")
except KeyError as ke:
    print(f"❌ Unexpected response format — missing key: {ke}")
except Exception as e:
    print(f"❌ Database or other error: {e}")

# %%
