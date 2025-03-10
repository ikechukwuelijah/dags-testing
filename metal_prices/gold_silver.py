
#%% step 1
import requests
import pandas as pd
import psycopg2
import os
# API Request
url = "https://gold-price-live.p.rapidapi.com/get_metal_prices"
headers = {
    "x-rapidapi-key": "7b66ced988msh253ab4a526f3148p1eed78jsn4d8bcaa48242",  # Use environment variable for security
    "x-rapidapi-host": "gold-price-live.p.rapidapi.com"
}

response = requests.get(url, headers=headers)
print(response.json())

#%% step 2
# Transform the JSON data to a DataFrame
data = response.json()
df = pd.DataFrame(list(data.items()), columns=['Metal', 'Price'])
print(df)

#%% step 3 load data to postgres db using psycopg2

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
    CREATE TABLE IF NOT EXISTS metal_prices (
        id SERIAL PRIMARY KEY,
        metal VARCHAR(50),
        price NUMERIC
    );
    """
    cursor.execute(create_table_query)
    conn.commit()

    # Insert data into the table
    insert_query = """
    INSERT INTO metal_prices (metal, price)
    VALUES (%s, %s)
    """
    for _, row in df.iterrows():
        cursor.execute(insert_query, (row['Metal'], row['Price']))

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
