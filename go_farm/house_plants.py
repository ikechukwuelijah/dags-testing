
#%% fetch data from API
import pandas as pd
import requests
import json
import psycopg2

url = "https://house-plants2.p.rapidapi.com/all-lite"

headers = {
	"x-rapidapi-key": "f38eae887bmsh5211e33c97c1c50p125cafjsnec52eb060a05",
	"x-rapidapi-host": "house-plants2.p.rapidapi.com"
}

response = requests.get(url, headers=headers)

print(response.json())
# %%
data = response.json()

# Transforming API data to Pandas DataFrame
df = pd.DataFrame([{
    'category': plant.get('Categories', ''),
    'common_name': ', '.join(plant.get('Common name', [])) if plant.get('Common name') else None,
    'latin_name': plant.get('Latin name', ''),
    'family': plant.get('Family', ''),
    'origin': ', '.join(plant.get('Origin', [])) if plant.get('Origin') else None,
    'climate': plant.get('Climat', ''),
    'image_url': plant.get('Img', ''),
    'zone': ', '.join(plant.get('Zone', [])) if plant.get('Zone') else None
} for plant in data])

print(df.head())  # Check the transformed DataFrame
# %%
# Database connection parameters
DB_NAME = "dwh"
DB_USER = "ikeengr"
DB_PASSWORD = "DataEngineer247"
DB_HOST = "89.40.0.150"
DB_PORT = "5432"

# Establish connection to PostgreSQL
conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)
cursor = conn.cursor()

# Create table if not exists
create_table_query = """
CREATE TABLE IF NOT EXISTS house_plants (
    id SERIAL PRIMARY KEY,
    category TEXT,
    common_name TEXT,
    latin_name TEXT,
    family TEXT,
    origin TEXT,
    climate TEXT,
    image_url TEXT,
    zone TEXT,
    UNIQUE (latin_name, zone)  -- Prevent duplicate inserts
);
"""
cursor.execute(create_table_query)
conn.commit()

# Insert data
insert_query = """
INSERT INTO house_plants (category, common_name, latin_name, family, origin, climate, image_url, zone)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (latin_name, zone) DO NOTHING;
"""
records = df.replace({pd.NA: None}).values.tolist()
cursor.executemany(insert_query, records)
conn.commit()

# Close connection
cursor.close()
conn.close()

print("Data successfully loaded into PostgreSQL!")

# %%
