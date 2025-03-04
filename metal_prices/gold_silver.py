import requests
import pandas as pd
import psycopg2
import os

# API Request
url = "https://gold-price-live.p.rapidapi.com/get_metal_prices"
headers = {
    "x-rapidapi-key": os.getenv("RAPIDAPI_KEY"),  # Use environment variable for security
    "x-rapidapi-host": "gold-price-live.p.rapidapi.com"
}

response = requests.get(url, headers=headers)

# Check if API request was successful
if response.status_code == 200:
    data = response.json()  # Example: {'gold': 2918.21, 'silver': 31.8791}
    print("API Response:", data)
else:
    print("Error:", response.status_code, response.text)
    data = {}

# Transform JSON Data into DataFrame
df = pd.DataFrame(list(data.items()), columns=['metal_name', 'price'])
df["currency"] = "USD"  # Assuming all prices are in USD
#%% step 3 load data to postgres db using psycopg2
# Database connection details
db_host = "89.40.0.150"
db_name = "dwh"
db_user = "ikeengr"
db_password = "DataEngineer247"
db_port = "5432"

try:
    # Establish a connection to the PostgreSQL database
    conn = psycopg2.connect(
        host=db_host,
        dbname=db_name,
        user=db_user,
        password=db_password,
        port=db_port
    )
    # Create a cursor object
    cur = conn.cursor()

    # Create table if not exists
    create_table_query = """
    CREATE TABLE IF NOT EXISTS metal_prices (
        metal_name VARCHAR(50),
        price FLOAT,
        currency VARCHAR(3)
    )
    """
    cur.execute(create_table_query)
    conn.commit()

    # Insert data into the table
    for index, row in df.iterrows():
        insert_query = """
        INSERT INTO metal_prices (metal_name, price, currency)
        VALUES (%s, %s, %s)
        """
        cur.execute(insert_query, (row['metal_name'], row['price'], row['currency']))

    conn.commit()

    print("Data successfully loaded into the database.")

except Exception as e:
    print("Error:", e)

finally:
    # Close the cursor and the connection
    cur.close()
    conn.close()


# %%
