
#%% 
import requests
import pandas as pd
import psycopg2

# -------------------------------------------
# 1. Fetch malaria data from WHO GHO API
# -------------------------------------------
url = "https://ghoapi.azureedge.net/api/WHOSIS_000015"

response = requests.get(url)
data = response.json()

# -------------------------------------------
# 2. Transform JSON response into DataFrame
# -------------------------------------------
malaria_data = []
for record in data.get("value", []):
    malaria_data.append({
        "Country": record.get("SpatialDim"),
        "Year": record.get("TimeDim"),
        "Cases": record.get("NumericValue")
    })

df = pd.DataFrame(malaria_data)
print(df.head())

# -------------------------------------------
# 3. PostgreSQL connection parameters
# -------------------------------------------
db_params = {
    "dbname": "dwh",
    "user": "ikeengr",
    "password": "DataEngineer247",
    "host": "89.40.0.150",
    "port": "5432"
}

# -------------------------------------------
# 4. Connect to PostgreSQL using psycopg2
# -------------------------------------------
try:
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    print("✅ Connected to PostgreSQL database.")

    # -------------------------------------------
    # 5. Create table (if not exists)
    # -------------------------------------------
    create_table_query = """
    CREATE TABLE IF NOT EXISTS malaria_stats (
        Country VARCHAR,
        Year INT,
        Cases FLOAT
    );
    """
    cursor.execute(create_table_query)
    conn.commit()

    # -------------------------------------------
    # 6. Insert DataFrame rows into the table
    # -------------------------------------------
    for index, row in df.iterrows():
        insert_query = """
        INSERT INTO malaria_stats (Country, Year, Cases)
        VALUES (%s, %s, %s)
        """
        cursor.execute(insert_query, (row['Country'], row['Year'], row['Cases']))

    # Commit all inserts
    conn.commit()
    print("✅ Data inserted into 'malaria_stats' table successfully.")

except Exception as e:
    print(f"❌ Error occurred: {e}")

finally:
    # Close DB connection
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()
    print("🔒 Database connection closed.")
