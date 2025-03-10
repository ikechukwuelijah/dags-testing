
#%% step 1
import requests
import json
import pandas as pd
import psycopg2
from psycopg2 import sql

url = "https://movies-tv-shows-database.p.rapidapi.com/"

querystring = {"year":"2025","page":"1"}

headers = {
	"x-rapidapi-key": "b138837176msh66610dbb568050ap1aec3ejsn32d522c6ac16",
	"x-rapidapi-host": "movies-tv-shows-database.p.rapidapi.com",
	"Type": "get-shows-byyear"
}

response = requests.get(url, headers=headers, params=querystring)

print(response.json())

data = response.json()
# %% step 2
# Convert to DataFrame
if 'movie_results' in data:
    df = pd.DataFrame(data['movie_results'])
    print(df)  # Display DataFrame
else:
    print("No movie results found.")
# %% step 3
# Connect to PostgreSQL database
# Database connection details (update these with your credentials)
DB_NAME = "dwh"
DB_USER = "ikeengr"
DB_PASSWORD = "DataEngineer247"
DB_HOST = "89.40.0.150"  # Use "localhost" if running locally
DB_PORT = "5432"  # Default PostgreSQL port

# Initialize variables
conn = None
cursor = None

try:
    # Establish connection
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    cursor = conn.cursor()
    print("Connected to the database successfully!")

    # Create table if not exists
    create_table_query = """
    CREATE TABLE IF NOT EXISTS movies (
        id SERIAL PRIMARY KEY,
        title TEXT,
        year INT,
        imdb_id TEXT UNIQUE
    );
    """
    cursor.execute(create_table_query)
    conn.commit()
    print("Table created or already exists.")

    # Convert DataFrame to list of tuples
    movie_data = [
        (row['title'], int(row['year']), row['imdb_id'])
        for index, row in df.iterrows()
    ]

    # Insert data into PostgreSQL
    insert_query = """
    INSERT INTO movies (title, year, imdb_id) 
    VALUES (%s, %s, %s) 
    ON CONFLICT (imdb_id) DO NOTHING;
    """
    cursor.executemany(insert_query, movie_data)
    conn.commit()
    print("Data inserted successfully!")

except Exception as e:
    print(f"Error: {e}")

finally:
    # Close cursor and connection safely
    if cursor is not None:
        cursor.close()
    if conn is not None:
        conn.close()
    print("Database connection closed.")
# %%
