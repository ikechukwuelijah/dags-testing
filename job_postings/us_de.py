#%%
import requests
import pandas as pd
import psycopg2

url = "https://job-posting-feed-api.p.rapidapi.com/active-ats-meili"

querystring = {"search":"data engineer","title_search":"false","description_type":"html","location_filter":"United States"}

headers = {
	"x-rapidapi-key": "f38eae887bmsh5211e33c97c1c50p125cafjsnec52eb060a05",
	"x-rapidapi-host": "job-posting-feed-api.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)

print(response.json())
data = response.json()

#%% Step 2: Transform Data
# Convert to DataFrame
job_list = data.get('hits', [])
df = pd.DataFrame(job_list, columns=['title', 'locations_derived', 'date_posted'])

# Fix date_posted conversion issue
df['date_posted'] = df['date_posted'].str.replace(r'\+00:00$', '', regex=True)  # Remove timezone
df['date_posted'] = pd.to_datetime(df['date_posted'], errors='coerce')

# Extract first location
df['location'] = df['locations_derived'].apply(lambda x: x[0] if isinstance(x, list) and x else None)

# Drop locations_derived
df.drop(columns=['locations_derived'], inplace=True)

print(df)

#%% Step 3: Load Data into PostgreSQL
# Database connection parameters
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
    cur = conn.cursor()
    print("Connected to PostgreSQL successfully.")

    # Create table if it does not exist
    cur.execute("""
        CREATE TABLE IF NOT EXISTS us_de_jobs (
            id SERIAL PRIMARY KEY,
            title TEXT,
            location TEXT,
            date_posted DATE
        )
    """)

    # Insert data into the database, replacing NaT with NULL
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO us_de_jobs (title, location, date_posted)
            VALUES (%s, %s, %s)
        """, (row['title'], row['location'], row['date_posted'] if pd.notna(row['date_posted']) else None))

    # Commit changes and close connection
    conn.commit()
    print("Data inserted successfully.")

except Exception as e:
    print(f"Error: {e}")

finally:
    if cur:
        cur.close()
    if conn:
        conn.close()
    print("PostgreSQL connection closed.")

# %%
