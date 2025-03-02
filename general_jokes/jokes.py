import requests
import pandas as pd
import psycopg2
from psycopg2 import sql

# PostgreSQL Database Configuration
DB_NAME = "dwh"
DB_USER = "ikeengr"
DB_PASSWORD = "DataEngineer247"
DB_HOST = "89.40.0.150"  # Change if using a remote server
DB_PORT = "5432"  # Default PostgreSQL port

# API URL to fetch all jokes
url = "https://official-joke-api.appspot.com/jokes/general/ten"

# Fetch data from API
response = requests.get(url)

if response.status_code == 200:
    jokes = response.json()  # Convert response to JSON
    df = pd.DataFrame(jokes)  # Convert JSON to DataFrame
else:
    print("Failed to retrieve data")
    exit()

# Connect to PostgreSQL
try:
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    cursor = conn.cursor()
    print("Connected to PostgreSQL successfully!")
    
    # Create table if not exists
    create_table_query = """
    CREATE TABLE IF NOT EXISTS general_jokes (
        id SERIAL PRIMARY KEY,
        type TEXT,
        setup TEXT,
        punchline TEXT
    );
    """
    cursor.execute(create_table_query)
    conn.commit()
    
    # Insert Data into PostgreSQL
    insert_query = """
    INSERT INTO general_jokes (type, setup, punchline) VALUES (%s, %s, %s)
    """
    
    for _, row in df.iterrows():
        cursor.execute(insert_query, (row["type"], row["setup"], row["punchline"]))
    
    conn.commit()
    print("Data inserted successfully!")

except Exception as e:
    print("Error:", e)
finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()
    print("PostgreSQL connection closed.")


