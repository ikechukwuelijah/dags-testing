
#%% step 1
import requests
import json
import pandas as pd
import psycopg2
from psycopg2 import sql

url = "https://real-time-amazon-data.p.rapidapi.com/search"

querystring = {"query":"Fashion","page":"1","country":"US","sort_by":"BEST_SELLERS","product_condition":"ALL","is_prime":"false","deals_and_discounts":"NONE"}

headers = {
	"x-rapidapi-key": "b138837176msh66610dbb568050ap1aec3ejsn32d522c6ac16",
	"x-rapidapi-host": "real-time-amazon-data.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)

print(response.json())


# %% step 2
# Convert the 'products' list inside 'data' to a DataFrame
data = response.json()
if 'data' in data and 'products' in data['data']:
    df = pd.DataFrame(data['data']['products'])
    print(df.head())  # Display the first few rows
else:
    print("No product data found in the response.")

# %% step 3
# Define PostgreSQL connection details
DB_PARAMS = {
    "dbname": "dwh",
    "user": "ikeengr",
    "password": "DataEngineer247",
    "host": "89.40.0.150",
    "port": "5432"
}

def load_to_postgres(df):
    """Load API response DataFrame into PostgreSQL database."""
    
    if df.empty:
        print("No data available to insert.")
        return
    
    # Connect to PostgreSQL
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()

    # Create table if not exists
    create_table_query = """
    CREATE TABLE IF NOT EXISTS fashion_products (
        id SERIAL PRIMARY KEY,
        title TEXT,
        price TEXT,
        rating FLOAT,
        reviews TEXT,
        product_url TEXT
    );
    """
    cursor.execute(create_table_query)

    # Insert data into the table
    insert_query = """
    INSERT INTO fashion_products (title, price, rating, reviews, product_url)
    VALUES (%s, %s, %s, %s, %s);
    """

    for _, row in df.iterrows():
        cursor.execute(insert_query, (
            row.get("title"),
            row.get("price"),
            row.get("rating"),
            row.get("reviews"),
            row.get("product_url")
        ))

    # Commit changes and close connection
    conn.commit()
    cursor.close()
    conn.close()
    print("Data inserted successfully.")

# Call function with the DataFrame from Step 2
load_to_postgres(df)

