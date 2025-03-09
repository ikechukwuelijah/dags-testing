
#%% step 1
import requests
import json
import pandas as pd

# Define the base URLs for the categories
urls = {
    "general": "https://official-joke-api.appspot.com/jokes/general/ten",
    "programming": "https://official-joke-api.appspot.com/jokes/programming/ten"
}

# Fetch jokes from multiple categories
all_jokes = []
for url in urls.values():
    response = requests.get(url)
    if response.status_code == 200:
        jokes = response.json()
        print("JSON data from the API:", json.dumps(jokes, indent=4))  # Print JSON data for debugging
        all_jokes.extend(jokes)
    else:
        print(f"Failed to retrieve data from {url}")

# Print the combined jokes in JSON format
if all_jokes:
    print("Combined Jokes JSON:")
    print(json.dumps(all_jokes, indent=4))  # Display the combined JSON data with indentation for readability


#%% step 2
# Transform the JSON data to a DataFrame
df = pd.DataFrame(all_jokes)
print(df)


#%% step 3
# Load the data into a PostgreSQL database
import psycopg2
from psycopg2 import sql

# PostgreSQL connection details
db_config = {
    'dbname': 'dwh', 
    'user': 'ikeengr',        
    'password': 'DataEngineer247',    
    'host': '89.40.0.150',            
    'port': '5432'                
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
        cursor.execute(insert_query, (row['id'], row['setup'], row['punchline'], None))  # Use None for category if it's missing

    conn.commit()
    print("Data successfully loaded into PostgreSQL database.")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Close the database connection if it was established
    if conn:
        cursor.close()
        conn.close()

# %%
