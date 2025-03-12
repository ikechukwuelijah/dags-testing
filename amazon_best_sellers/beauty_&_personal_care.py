#%%
import requests
import pandas as pd
import psycopg2
import json
from psycopg2 import sql

# Step 1: Fetch Data from API
url = "https://real-time-amazon-data.p.rapidapi.com/search"
querystring = {
    "query": "Beauty & Personal Care",
    "page": "1",
    "country": "US",
    "sort_by": "BEST_SELLERS",
    "product_condition": "ALL",
    "is_prime": "false",
    "deals_and_discounts": "NONE"
}

headers = {
    "x-rapidapi-key": "b138837176msh66610dbb568050ap1aec3ejsn32d522c6ac16",
    "x-rapidapi-host": "real-time-amazon-data.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)
data = response.json()

#%%
# Step 2: Convert API response to DataFrame
if "data" in data and "products" in data["data"]:
    df = pd.DataFrame(data["data"]["products"])
    print(df.head())  # Display the first few rows
else:
    print("No product data found in the response.")
    df = pd.DataFrame()  # Create an empty DataFrame to prevent errors

# Step 3: Clean Data (Ensure Required Columns Exist)
expected_columns = ["title", "price", "rating", "reviews", "url"]

# Rename "url" to "product_url" to match the database column
if "url" in df.columns:
    df.rename(columns={"url": "product_url"}, inplace=True)

# Ensure all expected columns exist
for col in expected_columns:
    if col not in df.columns:
        df[col] = None  # Add missing columns with NULL values

print(df.head())  # Debug: Check column names

#%%
# Step 4: Load Data to PostgreSQL
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
    CREATE TABLE IF NOT EXISTS beauty_personal_care_products (
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
    INSERT INTO beauty_personal_care_products (title, price, rating, reviews, product_url)
    VALUES (%s, %s, %s, %s, %s);
    """

    for _, row in df.iterrows():
        cursor.execute(insert_query, (
            row["title"],
            row["price"],
            row["rating"],
            row["reviews"],
            row["product_url"]
        ))

    # Commit changes and close connection
    conn.commit()
    cursor.close()
    conn.close()
    print("Data inserted successfully.")

# Call function to load data into PostgreSQL
load_to_postgres(df)

# %%