import requests
import pandas as pd
import psycopg2
from psycopg2 import sql

# Define the base URLs for the categories
urls = {
    "general": "https://official-joke-api.appspot.com/jokes/general/ten",
    "programming": "https://official-joke-api.appspot.com/jokes/programming/ten"
}

# Fetch jokes from multiple categories
all_jokes = []
for category, url in urls.items():
    response = requests.get(url)
    if response.status_code == 200:
        jokes = response.json()
        for joke in jokes:
            joke['category'] = category  # Add a category label to each joke
        all_jokes.extend(jokes)
    else:
        print(f"Failed to retrieve data from {category} category")

# Convert combined data into a DataFrame
if all_jokes:
    df = pd.DataFrame(all_jokes)
    print("Jokes DataFrame:")
    print(df)  # Display the combined DataFrame
else:
    print("No jokes retrieved")
    exit()

# PostgreSQL connection details
db_config = {
    'dbname': 'dwh',  # Replace with your database name
    'user': 'ikeengr',         # Replace with your username
    'password': 'DataEngineer247',     # Replace with your password
    'host': '89.40.0.150',             # Replace with your host (e.g., localhost or an IP address)
    'port': '5432'                   # Replace with your port (default is 5432)
}

# Initialize the connection variable
conn = None

try:
    # Connect to PostgreSQL
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    # Create the table if it doesn't exist
    create_table_query = """
    CREATE TABLE IF NOT EXISTS jokes (
        id SERIAL PRIMARY KEY,
        joke_id INTEGER UNIQUE,
        setup TEXT,
        punchline TEXT,
        category VARCHAR(50)
    );
    """
    cursor.execute(create_table_query)
    conn.commit()

    # Insert jokes into the table
    insert_query = sql.SQL("""
    INSERT INTO jokes (joke_id, setup, punchline, category)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (joke_id) DO NOTHING;
    """)

    for _, row in df.iterrows():
        cursor.execute(insert_query, (row['id'], row['setup'], row['punchline'], row['category']))

    conn.commit()
    print("Data successfully loaded into PostgreSQL database.")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Close the database connection if it was established
    if conn:
        cursor.close()
        conn.close()