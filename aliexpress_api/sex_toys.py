
#%% fetch data from AliExpress API and load into PostgreSQL database
# Import required libraries
import requests
import pandas as pd
import json
import psycopg2
from psycopg2 import sql 

url = "https://aliexpress-business-api.p.rapidapi.com/textsearch.php"

querystring = {"keyWord":"sex toy","pageSize":"20","pageIndex":"1","country":"US","currency":"USD","lang":"en","filter":"orders","sortBy":"asc"}

headers = {
	"x-rapidapi-key": "b138837176msh66610dbb568050ap1aec3ejsn32d522c6ac16",
	"x-rapidapi-host": "aliexpress-business-api.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)

print(response.json())


# %%
# Extract JSON response and # Transform the 'itemList' into a DataFrame
data = response.json()
df = pd.DataFrame(data['data']['itemList'])

# Display the first few rows of the DataFrame
print(df.head())


#Load the data into a PostgreSQL database
#%%
# Database connection details
db_params = {
    "dbname": "dwh",
    "user": "ikeengr",
    "password": "DataEngineer247",
    "host": "89.40.0.150",
    "port": "5432"
}

# Connect to PostgreSQL
try:
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    
    # Create table if not exists
    create_table_query = """
    CREATE TABLE IF NOT EXISTS sex_toy (
        itemId VARCHAR PRIMARY KEY,
        title TEXT,
        originalPrice FLOAT,
        originalPriceCurrency VARCHAR(10),
        salePrice FLOAT,
        salePriceCurrency VARCHAR(10),
        discount VARCHAR(10),
        itemMainPic TEXT,
        score VARCHAR(10),
        targetSalePrice FLOAT,
        targetOriginalPrice FLOAT,
        cateId TEXT,
        orders TEXT
    );
    """
    cursor.execute(create_table_query)
    conn.commit()

    # Insert data into table
    insert_query = """
    INSERT INTO sex_toy (
        itemId, title, originalPrice, originalPriceCurrency, salePrice, salePriceCurrency, 
        discount, itemMainPic, score, targetSalePrice, targetOriginalPrice, cateId, orders
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (itemId) DO NOTHING;
    """

    # Convert DataFrame rows to list of tuples
    records = df[['itemId', 'title', 'originalPrice', 'originalPriceCurrency', 'salePrice', 
                  'salePriceCurrency', 'discount', 'itemMainPic', 'score', 'targetSalePrice', 
                  'targetOriginalPrice', 'cateId', 'orders']].values.tolist()

    # Execute batch insert
    cursor.executemany(insert_query, records)
    conn.commit()

    print(f"Inserted {cursor.rowcount} records successfully.")

except Exception as e:
    print("Error:", e)

finally:
    cursor.close()
    conn.close()

# %%
