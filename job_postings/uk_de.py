import requests
import pandas as pd
import psycopg2

# API details
url = "https://job-posting-feed-api.p.rapidapi.com/active-ats-meili"
querystring = {
    "search": "Data Engineer",
    "title_search": "true",
    "description_type": "html",
    "location_filter": "United Kingdom"
}
headers = {
    "x-rapidapi-key": "f38eae887bmsh5211e33c97c1c50p125cafjsnec52eb060a05",
    "x-rapidapi-host": "job-posting-feed-api.p.rapidapi.com"
}

# Fetch data from API
response = requests.get(url, headers=headers, params=querystring)
data = response.json()

# Process data
if 'hits' in data and isinstance(data['hits'], list):
    job_postings = data['hits']
    df = pd.DataFrame([{ 
        'job_title': job.get('title', ''), 
        'locations': ', '.join(job.get('locations_derived', [''])), 
        'date_posted': job.get('date_posted', None) 
    } for job in job_postings])
else:
    df = pd.DataFrame()

# Database connection details
db_params = {
    "dbname": "dwh",
    "user": "ikeengr",
    "password": "DataEngineer247",
    "host": "89.40.0.150",
    "port": "5432"
}

try:
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    # Check if table exists
    cursor.execute("SELECT to_regclass('public.uk_de_job_postings');")
    table_exists = cursor.fetchone()[0] is not None

    if not table_exists:
        cursor.execute("""
            CREATE TABLE uk_de_job_postings (
                id SERIAL PRIMARY KEY,
                job_title TEXT,
                locations TEXT,
                date_posted TIMESTAMP,
                CONSTRAINT uk_de_job_postings_unique UNIQUE (job_title, locations, date_posted)
            );
        """)
        conn.commit()

    # Insert data if DataFrame is not empty
    if not df.empty:
        insert_query = """
            INSERT INTO uk_de_job_postings (job_title, locations, date_posted)
            VALUES (%s, %s, %s)
            ON CONFLICT (job_title, locations, date_posted) DO NOTHING;
        """
        records = df.replace({pd.NA: None}).values.tolist()
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