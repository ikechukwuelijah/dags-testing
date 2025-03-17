
#%% fetch data from api
import requests
import pandas as pd
import psycopg2

# API details
url = "https://job-posting-feed-api.p.rapidapi.com/active-ats-meili"
querystring = {
    "search": "Data Engineer",
    "title_search": "true",
    "description_type": "html",
    "location_filter": "Canada"
}
headers = {
    "x-rapidapi-key": "b138837176msh66610dbb568050ap1aec3ejsn32d522c6ac16",
    "x-rapidapi-host": "job-posting-feed-api.p.rapidapi.com"
}

# Make the API request
response = requests.get(url, headers=headers, params=querystring)

print(response.json())

# transform data to dataframe
#%%
# Extract JSON response
data = response.json()

# Check if 'hits' key exists and is a list
if 'hits' in data and isinstance(data['hits'], list):
    job_postings = data['hits']

    # Extract relevant fields while handling missing data
    flattened_postings = []
    for job in job_postings:
        flattened_postings.append({
            'job_title': job.get('title', ''),  # Job title
            'locations': ', '.join(job.get('locations_derived', [''])),  # Join multiple locations
            'date_posted': job.get('date_posted', None)  # Date posted
        })

    # Convert to DataFrame
    df = pd.DataFrame(flattened_postings)
    print("DataFrame Created:\n", df.head())

else:
    print("Warning: 'hits' key not found or not a list in API response.")
    print("Actual Response:", data)
    df = pd.DataFrame()  # Create an empty DataFrame



#%% load data to postgresql 
# Database connection details
db_params = {
    "dbname": "dwh",
    "user": "ikeengr",
    "password": "DataEngineer247",
    "host": "89.40.0.150",
    "port": "5432"
}

# Connect to PostgreSQL and insert data
try:
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    # Create table if it does not exist
    create_table_query = """
    CREATE TABLE IF NOT EXISTS job_postings (
        id SERIAL PRIMARY KEY,
        job_title TEXT,
        locations TEXT,
        date_posted TIMESTAMP,
        CONSTRAINT unique_job_posting UNIQUE (job_title, locations, date_posted)
    );
    """
    cursor.execute(create_table_query)
    conn.commit()

    # Insert data into table if df is not empty
    if not df.empty:
        insert_query = """
        INSERT INTO job_postings (job_title, locations, date_posted)
        VALUES (%s, %s, %s)
        ON CONFLICT (job_title, locations, date_posted) DO NOTHING;
        """

        # Convert DataFrame rows to list of tuples, replacing NaN with None for SQL
        records = df.replace({pd.NA: None}).values.tolist()

        # Execute batch insert
        cursor.executemany(insert_query, records)
        conn.commit()

        print(f"Inserted {cursor.rowcount} records successfully.")
    else:
        print("No data to insert into the database.")

except psycopg2.Error as db_error:
    print("Database error:", db_error)

except Exception as e:
    print("Error:", e)

finally:
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()

 # %%
