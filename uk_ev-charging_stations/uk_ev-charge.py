import requests
import xml.etree.ElementTree as ET
import pandas as pd
import psycopg2

# Constants
API_URL = "https://api.openchargemap.io/v3/poi/?output=kml&countrycode=CA&maxresults=5000&key=c8548da8-d611-4aea-b4e2-bfe17944b989"

# PostgreSQL database credentials
DB_CONFIG = {
    "dbname": "",
    "user": "",
    "password": "",
    "host": "",
    "port": "5432"
}

def fetch_data():
    """Fetch data from Open Charge Map API."""
    try:
        response = requests.get(API_URL, timeout=30)
        response.raise_for_status()
        return response.content  # Return raw KML data
    except requests.RequestException as e:
        print("Error fetching data:", e)
        return None

def parse_kml(data):
    """Parse KML data and extract charging station details into a DataFrame."""
    try:
        root = ET.fromstring(data)
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
                
                # Coordinates format: longitude,latitude,altitude (altitude can be ignored)
                lon, lat, *_ = coordinates.split(",")
                stations.append({"name": name, "latitude": float(lat), "longitude": float(lon), "description": description})
        
        return pd.DataFrame(stations)
    except ET.ParseError as e:
        print("Error parsing KML:", e)
        return pd.DataFrame()


def insert_into_db(df):
    """Insert charging station data from DataFrame into PostgreSQL database."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # Create table if not exists
        cur.execute('''
            CREATE TABLE IF NOT EXISTS uk_ev_charging_stations (
                id SERIAL PRIMARY KEY,
                name TEXT,
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                description TEXT
            )
        ''')
        conn.commit()

        # Insert data
        for _, row in df.iterrows():
            print(f"Inserting row: {row}")  # Debug print
            cur.execute('''
                INSERT INTO uk_ev_charging_stations (name, latitude, longitude, description)
                VALUES (%s, %s, %s, %s)
            ''', (row["name"], row["latitude"], row["longitude"], row["description"]))

        conn.commit()
        print("Data inserted successfully!")
    except psycopg2.DatabaseError as e:
        print("Database error:", e)
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    # Step 1: Fetch Data
    kml_data = fetch_data()
    
    if kml_data:
        # Step 2: Parse KML Data into DataFrame
        df_stations = parse_kml(kml_data)
        print("DataFrame created:", df_stations)  # Debug print
        
        # Step 3: Insert Data into PostgreSQL
        if not df_stations.empty:
            insert_into_db(df_stations)
        else:
            print("No stations found in the data.")

# %%
