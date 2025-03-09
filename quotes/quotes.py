
#%% step 1
import requests
import pandas as pd 

# API Request
url = "https://quotes15.p.rapidapi.com/quotes/random/"
headers = {
    "x-rapidapi-key": "7b66ced988msh253ab4a526f3148p1eed78jsn4d8bcaa48242",  # Use environment variable for security
    "x-rapidapi-host": "quotes15.p.rapidapi.com"
}

response = requests.get(url, headers=headers)
data = response.json()
print(data)

#%% step 2
# Transform JSON data to a DataFrame
df = pd.DataFrame([{
    'quote_id': data['id'],
    'quote_content': data['content'],
    'quote_url': data['url'],
    'quote_language': data['language_code'],
    'originator_id': data['originator']['id'],
    'originator_name': data['originator']['name'],
    'originator_url': data['originator']['url'],
    'tags': ', '.join(data['tags'])  # Convert list of tags to a single string
}])

print(df)


#%% step 3
import psycopg2

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
    CREATE TABLE IF NOT EXISTS quotes (
        id SERIAL PRIMARY KEY,
        quote_id INTEGER UNIQUE,
        quote_content TEXT,
        quote_url TEXT,
        quote_language VARCHAR(10),
        originator_id INTEGER,
        originator_name VARCHAR(100),
        originator_url TEXT,
        tags TEXT
    );
    """
    cursor.execute(create_table_query)
    conn.commit()

    # Insert data into the table
    insert_query = """
    INSERT INTO quotes (quote_id, quote_content, quote_url, quote_language, originator_id, originator_name, originator_url, tags)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (quote_id) DO NOTHING;
    """
    for _, row in df.iterrows():
        cursor.execute(insert_query, (row['quote_id'], row['quote_content'], row['quote_url'], row['quote_language'], row['originator_id'], row['originator_name'], row['originator_url'], row['tags']))

    conn.commit()
    print("Data successfully loaded into PostgreSQL database.")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Close the database connection if it was established
    if conn:
        cursor.close()
        conn.close()

