#%%
import requests
import json
import pandas as pd
import psycopg2

url = "https://real-time-amazon-data.p.rapidapi.com/search"

querystring = {"query":"sex_toys","page":"1","country":"US","sort_by":"BEST_SELLERS","product_condition":"ALL","is_prime":"false","deals_and_discounts":"NONE"}

headers = {
	"x-rapidapi-key": "f38eae887bmsh5211e33c97c1c50p125cafjsnec52eb060a05",
	"x-rapidapi-host": "real-time-amazon-data.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)

print(response.json())
# %%
# Convert response to JSON
data = response.json()

# Extract product data
products = data.get("data", {}).get("products", [])

# Convert to DataFrame
df = pd.DataFrame(products)

# Display the DataFrame
print(df.head())

# %%
# Database connection details
DB_NAME = "dwh"
DB_USER = "ikeengr"
DB_PASSWORD = "DataEngineer247"
DB_HOST = "89.40.0.150"  # Example: "localhost" or cloud instance
DB_PORT = "5432"  # Default PostgreSQL port

# Connect to PostgreSQL
try:
    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
    )
    cursor = conn.cursor()
    print("Connected to the database successfully!")
except Exception as e:
    print("Error connecting to the database:", e)
    exit()

# Create table if it doesn't exist
create_table_query = """
CREATE TABLE IF NOT EXISTS amazon_sex_toy_products (
    asin TEXT PRIMARY KEY,
    product_title TEXT,
    product_price TEXT,
    product_star_rating TEXT,
    product_num_ratings INTEGER,
    product_url TEXT,
    product_photo TEXT,
    sales_volume TEXT,
    delivery TEXT
);
"""
cursor.execute(create_table_query)
conn.commit()

# Insert data into PostgreSQL
for _, row in df.iterrows():
    insert_query = """
    INSERT INTO amazon_sex_toy_products (
        asin, product_title, product_price, product_star_rating, 
        product_num_ratings, product_url, product_photo, sales_volume, delivery
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (asin) DO NOTHING;
    """
    values = (
        row["asin"], row["product_title"], row["product_price"],
        row["product_star_rating"], row["product_num_ratings"],
        row["product_url"], row["product_photo"], row["sales_volume"],
        row["delivery"]
    )
    
    cursor.execute(insert_query, values)

# Commit and close connection
conn.commit()
cursor.close()
conn.close()
print("Data successfully inserted into PostgreSQL!")

# %%
