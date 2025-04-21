#%%
import requests
import pandas as pd
import psycopg2

# API request
url = "https://quotes-api12.p.rapidapi.com/quotes/random"
querystring = {"type": "success"}
headers = {
    "x-rapidapi-key": "efbc12a764msh39a81e663d3e104p1e76acjsn337fd1d56751",
    "x-rapidapi-host": "quotes-api12.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)
data = response.json()

# Convert to DataFrame
df = pd.DataFrame([data])  # ensure it's in list format

#%% Database connection credentials
conn = psycopg2.connect(
    dbname="dwh",
    user="ikeengr",
    password="DataEngineer247",
    host="89.40.0.150",
    port="5432"
)

cur = conn.cursor()

# Create table if not exists
cur.execute("""
    CREATE TABLE IF NOT EXISTS success (
        id SERIAL PRIMARY KEY,
        quote TEXT,
        author TEXT,
        type TEXT
    )
""")

# Insert the data
cur.execute("""
    INSERT INTO success (quote, author, type)
    VALUES (%s, %s, %s)
""", (data['quote'], data['author'], data['type']))

# Commit changes and close connection
conn.commit()
cur.close()
conn.close()

print("Quote inserted successfully into 'success' table.")
# %%
