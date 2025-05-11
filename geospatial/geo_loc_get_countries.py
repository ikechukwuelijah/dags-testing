import requests
import pandas as pd
import psycopg2

# -------------------------------
# CONFIGURATION
# -------------------------------

DB_CONFIG = {
    "dbname": "dwh",
    "user": "ikeengr",
    "password": "DataEngineer247",
    "host": "89.40.0.150",
    "port": "5432"
}

# -------------------------------
# 1. EXTRACT
# -------------------------------

def extract_country_data():
    url = "https://geo-location-data1.p.rapidapi.com/geo/get-countries"
    querystring = {"pageSize": "20", "page": "1"}

    headers = {
        "x-rapidapi-key": "e7cbee4ae1mshd0704b6dcc65548p1632e3jsncb04d537a049",
        "x-rapidapi-host": "geo-location-data1.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)
    response.raise_for_status()
    
    print(response.json())  # Optional for debug
    return response.json().get("data", [])


# -------------------------------
# 2. TRANSFORM
# -------------------------------

def transform_to_dataframe(raw_data):
    df = pd.DataFrame(raw_data)

    # Normalize and select only required columns
    columns = ['name', 'iso2', 'iso3', 'currency', 'capital', 'population', 'area_km2']
    for col in columns:
        if col not in df.columns:
            df[col] = None

    df = df[columns]
    return df


# -------------------------------
# 3. LOAD
# -------------------------------

def load_to_postgres(df):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Create table if not exists
    cur.execute("""
    CREATE TABLE IF NOT EXISTS geo_spatial_countries (
        id SERIAL PRIMARY KEY,
        name TEXT,
        iso2 TEXT,
        iso3 TEXT,
        currency TEXT,
        capital TEXT,
        population BIGINT,
        area_km2 REAL
    )
    """)
    conn.commit()

    # Insert rows
    for _, row in df.iterrows():
        cur.execute("""
        INSERT INTO geo_spatial_countries (name, iso2, iso3, currency, capital, population, area_km2)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            row['name'], row['iso2'], row['iso3'],
            row['currency'], row['capital'],
            row['population'], row['area_km2']
        ))

    conn.commit()
    cur.close()
    conn.close()
    print("Data successfully loaded to PostgreSQL.")


# -------------------------------
# RUN ETL
# -------------------------------

if __name__ == "__main__":
    raw_data = extract_country_data()
    df = transform_to_dataframe(raw_data)
    load_to_postgres(df)
